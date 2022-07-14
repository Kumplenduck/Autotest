SELECT COUNT(1)
  FROM (SELECT send_date, MAX(amount) AS amnt
          FROM (  SELECT send_date, COUNT(t_in.tr_id) AS amount
                    FROM ykram_samples_onebank.transactions t
                    JOIN
                    (  SELECT tr_id, send_date
                         FROM ykram_samples_onebank.client_tr ct
                        WHERE send_date >= '2022-03-11'
                          AND send_date <= '2022-03-14'
                     GROUP BY tr_id, send_date) t_in
                      ON t.tr_id = t_in.tr_id
                   WHERE send_bank_id = 3
                GROUP BY send_date
                   UNION ALL
                  SELECT '2022-03-11', 0
                   UNION ALL
                  SELECT '2022-03-12', 0
                   UNION ALL
                  SELECT '2022-03-13', 0
                   UNION ALL
                  SELECT '2022-03-14', 0) tbl
GROUP BY send_date) dm