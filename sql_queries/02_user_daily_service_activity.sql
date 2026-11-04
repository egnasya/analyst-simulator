-- Определение дневной активности пользователей в ленте и мессенджере
-- has_feed / has_message показывают факт использования сервиса (true/false)

SELECT
  user_id,
  event_date,
  country,
  age,
  os,
  gender,
  BIT_OR(has_feed) AS has_feed,
  BIT_OR(has_message) AS has_message
FROM (
    -- Пользователь совершил хотя бы одно действие в ленте
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
        true AS has_feed, 
        false AS has_message, 
        COUNT(*) AS count_actions
    FROM simulator_20261020.feed_actions 
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

    UNION ALL

    -- Пользователь совершил хотя бы одно действие в мессенджере
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
        false AS has_feed, 
        true AS has_message, 
        COUNT(*) AS count_actions
    FROM simulator_20261020.message_actions
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    )

-- Объединение активности из обоих источников:
-- BIT_OR возвращает true, если пользователь использовал соответствующий сервис
GROUP BY 1, 2, 3, 4, 5, 6