-- Когортный анализ пользователей, привлечённых 15.10.2026.
-- Сравнение рекламного (ads) и органического (organic) источников
-- по Retention, количеству действий и активности пользователей

WITH cohort AS (
    -- Формирование когорт по дате первой активности
    -- argMin определяет источник привлечения на момент первого действия
    SELECT
        user_id,
        MIN(toDate(time)) AS start_date,
        argMin(source, time) AS source
    FROM simulator_20261020.feed_actions
    GROUP BY user_id
    HAVING start_date = '2026-10-15'
),

cohort_size AS (
    -- Размер каждой когорты: количество привлечённых пользователей
    SELECT
        source,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM cohort
    GROUP BY source
),

user_daily AS (
    -- Количество действий каждого пользователя в каждый день жизни когорты
    -- lifetime_day = 0 соответствует дню привлечения
    SELECT
        c.user_id,
        c.source,
        dateDiff('day', c.start_date, toDate(f.time)) AS lifetime_day,
        COUNT(*) AS actions
    FROM cohort c
    INNER JOIN simulator_20261020.feed_actions f
        ON c.user_id = f.user_id
    WHERE toDate(f.time) >= c.start_date
    GROUP BY
        c.user_id,
        c.source,
        lifetime_day
),

daily_metrics AS (
    -- Дневные метрики активности когорт:
    -- активные пользователи, общее количество действий и среднее количество 
    -- действий на активного пользователя
    SELECT
        source,
        lifetime_day,
        COUNT(DISTINCT user_id) AS active_users,
        SUM(actions) AS total_actions,
        AVG(actions) AS avg_actions_per_active_user
    FROM user_daily
    GROUP BY source, lifetime_day
),

user_cumulative AS (
    -- Накопительное количество активных дней каждого пользователя
    -- от начала когорты до соответствующего lifetime_day
    SELECT
        source,
        lifetime_day,
        user_id,
        SUM(1) OVER (
            PARTITION BY source, user_id
            ORDER BY lifetime_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS active_days
    FROM user_daily
),

cumulative_metrics AS (
    -- Среднее накопительное количество активных дней
    -- среди пользователей, активных в соответствующий lifetime_day
    SELECT
        source,
        lifetime_day,
        AVG(active_days) AS avg_active_days
    FROM user_cumulative
    GROUP BY source, lifetime_day
)

-- Объединение метрик и расчёт итоговых показателей когорт
SELECT
    d.source AS source,
    addDays(toDate('2026-10-15'), d.lifetime_day) AS date,
    d.lifetime_day AS lifetime_day,
    cs.cohort_size,
    d.active_users,

    -- Retention = доля активных пользователей от исходного размера когорты
    ROUND(d.active_users * 1.0 / cs.cohort_size, 4) AS retention,

    d.total_actions,

    -- Среднее количество действий на одного привлечённого пользователя
    ROUND(d.total_actions * 1.0 / cs.cohort_size, 2) AS avg_actions_per_user,

    -- Среднее количество действий на одного активного пользователя
    ROUND(d.avg_actions_per_active_user, 2) AS avg_actions_per_active_user,

    ROUND(cm.avg_active_days, 2) AS avg_active_days
FROM daily_metrics d
JOIN cohort_size cs USING (source)
JOIN cumulative_metrics cm
    ON d.source = cm.source
   AND d.lifetime_day = cm.lifetime_day
ORDER BY date, d.source