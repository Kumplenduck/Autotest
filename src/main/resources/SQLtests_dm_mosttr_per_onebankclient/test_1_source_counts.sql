WITH one_send_bank
  AS (  SELECT sender_id
          FROM ykram_samples_onebank.client_tr ct
               JOIN ykram_samples_onebank.transactions t
               ON ct.tr_id = t.tr_id
      GROUP BY sender_id
        HAVING COUNT(DISTINCT send_bank_id) = 1),
tr_number
  AS (  SELECT sender_id,
               COUNT(ct.tr_id) AS tr_amount,
               SUM(tr_sum) AS tot_sum,
               SUM(bank_ch_sum) AS tot_charges
          FROM ykram_samples_onebank.client_tr ct
               JOIN ykram_samples_onebank.transactions t
               ON ct.tr_id = t.tr_id
               JOIN ykram_samples_onebank.tr_sum_ch ts
               ON t.tr_id = ts.tr_id
         WHERE sender_id IN
               (SELECT sender_id
                  FROM one_send_bank)
      GROUP BY sender_id, send_bank_id)
SELECT COUNT(1)
  FROM (SELECT last_nm, first_nm, mid_nm, tr_amount, tot_sum, tot_charges
  	  FROM ykram_samples_onebank.client c
          JOIN tr_number tn
            ON tn.sender_id = c.client_id
         WHERE tr_amount =
               (SELECT MAX(tr_amount)
                  FROM tr_number)) dm