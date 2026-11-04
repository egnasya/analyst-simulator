-- Сегментация пользователей по изменению активности в ленте
-- 23–24 октября 2026 года и последующему поведению до 02.11.2026
--
-- Сегменты:
-- both_days — активны и 23, и 24 октября
-- returned_later — активны 23 октября, неактивны 24 октября, но вернулись 
--                  в период 25.10–02.11
-- no_return — активны 23 октября, но не возвращались с 24.10 по 02.11
-- only_24oct — активны 24 октября, но не активны 23 октября
-- 23oct — вся аудитория, активная 23 октября

SELECT DISTINCT
    t.user_id,
    t.source,
    t.user_segment,
    fa.country,
    fa.city,
    fa.age,
    fa.os,
    fa.gender
FROM (
    -- Пользователи, активные в оба дня
    SELECT
        user_id,
        source,
        'both_days' AS user_segment
    FROM (
        SELECT user_id, source
        FROM simulator_20261020.feed_actions
        WHERE time::date = '2026-10-23'

        INTERSECT

        SELECT user_id, source
        FROM simulator_20261020.feed_actions
        WHERE time::date = '2026-10-24'
    )

    UNION ALL

    -- Пользователи, пропустившие 24 октября, но вернувшиеся в ленту в последующие дни
    SELECT
        user_id,
        source,
        'returned_later' AS user_segment
    FROM (
      (
      SELECT user_id, source
      FROM simulator_20261020.feed_actions
      WHERE time::date = '2026-10-23'

      EXCEPT

      SELECT user_id, source
      FROM simulator_20261020.feed_actions
      WHERE time::date = '2026-10-24'
      )

    INTERSECT 

    SELECT user_id, source
    FROM simulator_20261020.feed_actions
    WHERE time::date >= '2026-10-25' AND time::date <= '2026-11-02'
    )

    UNION ALL

    -- Пользователи, активные 23 октября, не проявившие активности ни 24 октября, ни в 
    -- последующие дни
    SELECT
        user_id,
        source,
        'no_return' AS user_segment
    FROM (
      SELECT user_id, source
      FROM simulator_20261020.feed_actions
      WHERE time::date = '2026-10-23'

      EXCEPT

      SELECT user_id, source
      FROM simulator_20261020.feed_actions
      WHERE time::date >= '2026-10-24' AND time::date <= '2026-11-02'
    )

    UNION ALL 

    -- Пользователи, появившиеся 24 октября, но не активные 23 октября
    SELECT
      user_id,
      source,
      'only_24oct' AS user_segment
    FROM (
      SELECT user_id, source
      FROM simulator_20261020.feed_actions
      WHERE time::date = '2026-10-24'

      EXCEPT

      SELECT user_id, source
      FROM simulator_20261020.feed_actions
      WHERE time::date = '2026-10-23'
    )

    UNION ALL 

    -- Полная аудитория пользователей, активных 23 октября
    -- Этот сегмент пересекается с both_days, returned_later и no_return
    SELECT
      user_id,
      source,
      '23oct' AS user_segment
    FROM (
      SELECT user_id, source
      FROM simulator_20261020.feed_actions
      WHERE time::date = '2026-10-23'
    )

) t

-- Добавление социально-демографических и технических характеристик
-- any() выбирает одно значение атрибута пользователя из истории действий
JOIN (
    SELECT
        user_id,
        any(country) AS country,
        any(city) AS city,
        any(age) AS age,
        any(os) AS os,
        any(gender) AS gender
    FROM simulator_20261020.feed_actions
    GROUP BY user_id
) fa
    ON fa.user_id = t.user_id