SELECT tr_amount, tot_sum, tot_charges
  FROM ykram_samples_onebank.dm_mosttr_per_onebankclient
 WHERE last_nm LIKE 'B%'