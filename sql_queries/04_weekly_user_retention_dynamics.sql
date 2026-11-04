-- Недельная динамика аудитории ленты:
-- new — первая активность в рассматриваемой неделе относительно предыдущей
-- retained — активность и в текущей, и в предыдущей неделе
-- gone — активность в предыдущей неделе, но отсутствие активности в текущей
--
-- Для визуализации ушедшие пользователи отображаются отрицательными значениями

-- Ушедшие пользователи (gone)
SELECT
  this_week,
  status,
  COUNT(DISTINCT user_id) * -1.0 AS number_of_users
FROM (
  SELECT
    user_id,

    -- Массив уникальных недель активности пользователя
    groupUniqArray(toMonday(time)) AS weeks_visited,

    -- Для каждой недели активности проверяем наличие активности
    -- в следующей неделе
    arrayJoin(weeks_visited) AS previous_week,
    addWeeks(previous_week, 1) AS this_week,

    CASE 
      WHEN has(weeks_visited, this_week) = 0 THEN 'gone'
    END AS status
  FROM simulator_20261020.feed_actions 
  GROUP BY user_id
)
WHERE status = 'gone'
GROUP BY this_week, status, previous_week

UNION ALL

-- Новые и сохранившиеся пользователи
SELECT
  this_week,
  status,
  COUNT(DISTINCT user_id) * 1.0 AS number_of_users
FROM (
  SELECT
    user_id,
    groupUniqArray(toMonday(time)) AS weeks_visited,

    -- Для каждой недели активности определяем,
    -- был ли пользователь активен в предыдущую неделю
    arrayJoin(weeks_visited) AS this_week,
    addWeeks(this_week, -1) AS previous_week,

    CASE 
      WHEN has(weeks_visited, previous_week) = 0 THEN 'new'
      WHEN has(weeks_visited, previous_week) = 1 THEN 'retained'
    END AS status
  FROM simulator_20261020.feed_actions 
  GROUP BY user_id
)
WHERE status IN ('retained', 'new')
GROUP BY this_week, status, previous_week