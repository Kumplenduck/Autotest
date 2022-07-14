SELECT COUNT(1)
  FROM ykram_samples_onebank.dm_sber_tramount_per_day
 WHERE send_date LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] _%'