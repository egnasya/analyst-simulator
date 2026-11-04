-- Объединение активности пользователей в ленте и мессенджере
-- Единая структура позволяет анализировать оба сервиса в одном датасете

SELECT
  user_id,
  event_date,
  gender,
  age,
  country,
  city,
  os,
  source,
  exp_group,
  action,
  count_actions
FROM (
  -- Агрегация действий пользователей в ленте по дням и типам действий
  SELECT 
    user_id, 
    time::date AS event_date, 
    gender, 
    age, 
    country, 
    city, 
    os, 
    source, 
    exp_group, 
    action, 
    COUNT(*) AS count_actions
  FROM simulator_20261020.feed_actions 
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

  UNION ALL

  -- Агрегация сообщений: все действия мессенджера объединяются
  -- в одну категорию action = 'message'
  SELECT 
    user_id, 
    time::date AS event_date, 
    gender, 
    age, 
    country, 
    city, 
    os, 
    source, 
    exp_group, 
    'message' AS action, 
    COUNT(*) AS count_actions
  FROM simulator_20261020.message_actions
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)