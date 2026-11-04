-- Подготовка данных для когортного анализа Retention
-- Для каждого пользователя определяется дата первой активности и источник привлечения
-- В результат попадают пользователи, впервые активные за последние 20 дней

SELECT 
  toString(date) AS date,
  toString(start_date) AS start_date,
  source,
  COUNT(user_id) AS active_users
FROM 
  (
    -- Уникальные даты активности каждого пользователя
    SELECT DISTINCT user_id, time::date AS date
    FROM simulator_20261020.feed_actions
  ) u
  JOIN 
  (
    -- Определение даты первой активности и источника привлечения пользователя
    SELECT user_id, MIN(time::date) AS start_date, source
    FROM simulator_20261020.feed_actions 
    GROUP BY user_id, source
    HAVING start_date >= today() - 20
  ) cohort ON u.user_id = cohort.user_id

GROUP BY date, start_date, source