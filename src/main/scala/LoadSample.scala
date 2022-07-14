import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, StructField, StructType}


object LoadSample extends App {
  val spark = SparkSession.builder()
    .master("yarn")
    //    .master("local[4]")
    .appName("LoadSample")
    .enableHiveSupport()
    .config("hive.metastore.uris", "thrift://dn01:9083")
    .getOrCreate()

  val path_Bank = "file:///media/ykramarenko/resources/samples/sample_bnk_clnt_tr.bank.csv"
  val path_Client = "file:///media/ykramarenko/resources/samples/sample_bnk_clnt_tr.client.csv"
  val path_Client_tr = "file:///media/ykramarenko/resources/samples/sample_bnk_clnt_tr.client_tr.csv"
  val path_Document = "file:///media/ykramarenko/resources/samples/sample_bnk_clnt_tr.document.csv"
  val path_Payment_system = "file:///media/ykramarenko/resources/samples/sample_bnk_clnt_tr.payment_system.csv"
  val path_Tr_sum_ch = "file:///media/ykramarenko/resources/samples/sample_bnk_clnt_tr.tr_sum_ch.csv"
  val path_Transactions = "file:///media/ykramarenko/resources/samples/sample_bnk_clnt_tr.transactions.csv"
  val path_dm_mosttr_per_onebankclient = "file:///media/ykramarenko/resources/samples" +
    "/sample_spec_tr_info.dm_mosttr_per_onebankclient.csv"
  val path_dm_sber_tramount_per_day = "file:///media/ykramarenko/resources/samples" +
    "/sample_spec_tr_info.dm_sber_tramount_per_day.csv"

  val bankSchema = new StructType(Array(
    StructField("bank_id", IntegerType, true),
    StructField("bank_nm", StringType, true),
    StructField("bank_ch_percent", DecimalType(3, 1), true)
  ))

  val clientSchema = new StructType(Array(
    StructField("client_id", IntegerType, true),
    StructField("last_nm", StringType, true),
    StructField("first_nm", StringType, true),
    StructField("mid_nm", StringType, true),
    StructField("birth_dt", StringType, true),
    StructField("doc_code", IntegerType, true),
    StructField("doc_num", StringType, true),
    StructField("exp_date", StringType, true)
  ))

  val client_trSchema = new StructType(Array(
    StructField("tr_id", LongType, true),
    StructField("isZero_ch", StringType, true),
    StructField("send_date", StringType, true),
    StructField("sender_id", IntegerType, true),
    StructField("receiver_id", IntegerType, true)
  ))

  val documentSchema = new StructType(Array(
    StructField("doc_code", IntegerType, true),
    StructField("doc_type", StringType, true),
    StructField("max_doc_age", IntegerType, true)
  ))

  val paymant_systemSchema = new StructType(Array(
    StructField("ps_code", IntegerType, true),
    StructField("ps_nm", StringType, true)
  ))

  val tr_sum_chSchema = new StructType(Array(
    StructField("tr_id", LongType, true),
    StructField("tr_sum", DecimalType(20, 2), true),
    StructField("bank_ch_sum", DecimalType(20, 2), true)
  ))

  val transactionsSchema = new StructType(Array(
    StructField("tr_id", IntegerType, true),
    StructField("send_bank_id", IntegerType, true),
    StructField("rec_bank_id", IntegerType, true),
    StructField("pay_sys", IntegerType, true),
    StructField("time_out", StringType, true),
    StructField("time_in", StringType, true)
  ))

  val dm_mosttr_per_onebankclientSchema = new StructType(Array(
    StructField("last_nm", StringType, true),
    StructField("first_nm", StringType, true),
    StructField("mid_nm", StringType, true),
    StructField("tr_amount", IntegerType, true),
    StructField("tot_sum", DecimalType(20, 2), true),
    StructField("tot_charges", DecimalType(20, 2), true)
  ))

  val dm_sber_tramount_per_day = new StructType(Array(
    StructField("send_date", StringType, true),
    StructField("amnt", IntegerType, true)))

  val dfBank = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(bankSchema)
    .csv(path_Bank)
  dfBank.createOrReplaceTempView("bnk_clnt_tr_bank")

  val dfClient = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(clientSchema)
    .csv(path_Client)
  dfClient.createOrReplaceTempView("bnk_clnt_tr_client")

  val dfClient_tr = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(client_trSchema)
    .csv(path_Client_tr)
  dfClient_tr.createOrReplaceTempView("bnk_clnt_tr_client_tr")

  val dfDocument = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(documentSchema)
    .csv(path_Document)
  dfDocument.createOrReplaceTempView("bnk_clnt_tr_document")

  val dfPayment_system = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(paymant_systemSchema)
    .csv(path_Payment_system)
  dfPayment_system.createOrReplaceTempView("bnk_clnt_tr_payment_system")

  val dfTr_sum_ch = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(tr_sum_chSchema)
    .csv(path_Tr_sum_ch)
  dfTr_sum_ch.createOrReplaceTempView("bnk_clnt_tr_tr_sum_ch")

  val dfTransactions = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(transactionsSchema)
    .csv(path_Transactions)

  val spec_tr_info_dm_mosttr_per_onebankclient = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(dm_mosttr_per_onebankclientSchema)
    .csv(path_dm_mosttr_per_onebankclient)

  val spec_tr_info_dm_sber_tramount_per_day = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(dm_sber_tramount_per_day)
    .csv(path_dm_sber_tramount_per_day)

  dfBank.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.bank")
  dfClient.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.client")
  dfClient_tr.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.client_tr")
  dfDocument.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.document")
  dfPayment_system.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.payment_system")
  dfTr_sum_ch.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.tr_sum_ch")
  dfTransactions.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.transactions")
  spec_tr_info_dm_mosttr_per_onebankclient.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.dm_mosttr_per_onebankclient")
  spec_tr_info_dm_sber_tramount_per_day.repartition(1).write.mode(SaveMode.Overwrite)
    .saveAsTable("yKram_samples_oneBank.dm_sber_tramount_per_day")
}
