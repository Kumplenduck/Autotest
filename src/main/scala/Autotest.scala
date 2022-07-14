import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.util.jar.JarFile
import scala.collection.mutable.ListBuffer

object Autotest extends App {
  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Test")
    .enableHiveSupport()
    .config("hive.metastore.uris", "thrift://dn01:9083")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //Номера тестов и названия для метода getPath

  val number_Test1 = "1"
  val number_Test2 = "2"
  val source = "source"
  val target = "target"

  //ListBuffer для OneBank и PerDay
  
  var pathListBufferOneBank = new ListBuffer[String]
  var pathListBufferPerDay = new ListBuffer[String]
  
  //получение списков с путями в директории src/main/resources
  
  val jarFile = new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath)
  if (jarFile.isFile) {
    val jar = new JarFile(jarFile)
    val entries = jar.entries
    while ( {
      entries.hasMoreElements
    }) {
      val name = entries.nextElement.getName
      if (name.startsWith("SQLtests_dm_mosttr_per_onebankclient/test")) {
        pathListBufferOneBank += name
      }
      else if (name.startsWith("SQLtests_dm_sber_tramount_per_day/test")) {
        pathListBufferPerDay += name
      }
    }
    jar.close()
  }
  
  // списки для метода getPaths() по двум видам OneBank и PerDay
  
  val pathListOneBank = pathListBufferOneBank.toList
  val counts_list_OneBank = pathListOneBank.filter(x => x.contains("counts"))
  val arrays_list_OneBank = pathListOneBank.filter(x => x.contains("arrays"))
  val ddl_list_OneBank = pathListOneBank.filter(x => x.contains("ddl"))
  val constants_list_OneBank = pathListOneBank.filter(x => x.contains("constants"))

  val pathListPerDay = pathListBufferPerDay.toList
  val counts_list_PerDay = pathListPerDay.filter(x => x.contains("counts"))
  val arrays_list_PerDay = pathListPerDay.filter(x => x.contains("arrays"))
  val ddl_list_PerDay = pathListPerDay.filter(x => x.contains("ddl"))
  val constants_list_PerDay = pathListPerDay.filter(x => x.contains("constants"))

  //тесты для OneBank
  //кол-во строк
  chekCounts(number_Test1, counts_list_OneBank)

  chekCounts(number_Test2, counts_list_OneBank)

 //атрибуты

  chekArrays(number_Test1, arrays_list_OneBank)

  chekArrays(number_Test2, arrays_list_OneBank)

  //структура данных

  chekDdl(number_Test1, ddl_list_OneBank)

  chekDdl(number_Test2, ddl_list_OneBank)

  //константы

  chekConstants(number_Test1, constants_list_OneBank)

  chekConstants(number_Test2, constants_list_OneBank)


  // тесты для PerDay

  //кол-во строк

  //ошибки в запросах
//  chekCounts(number_Test1, counts_list_OneBank)
//
//  chekCounts(number_Test2, counts_list_OneBank)

  //  //атрибуты

  chekArrays(number_Test1, arrays_list_PerDay)

  chekArrays(number_Test2, arrays_list_PerDay)

  //структура данных

  chekDdl(number_Test1, ddl_list_PerDay)

  chekDdl(number_Test2, ddl_list_PerDay)

  //константы

  chekConstants(number_Test1, constants_list_PerDay)

  chekConstants(number_Test2, constants_list_PerDay)

  //методы для сравнеия витрин и источников

  def chekConstants(testName: String, listName: List[String]): Unit = {
    val list_source = readSql(spark, getPaths(testName, source, listName))
          .collect().map(x => x.toSeq.toList.mkString(";")).toList
    val list_target = readString(getPaths(testName, target, listName))
    var chek = true

    try {
      if (list_source.length == list_target.length) {
        list_source.foreach(x => {
          if (!list_target.contains(x)) {
            chek = false
          }
        })
        try {
          if (!chek)
            throw new Exception(s"The source and the showcase do not match in test $testName")
        }
        catch {
          case exception: Exception => println(exception)
        }
      }
      else throw new Exception(s"Different table sizes in test $testName")
    }
    catch {
      case exception: Exception => println(exception)
    }
  }

  def chekDdl(testName: String, listName: List[String]): Unit = {
    val list_source = readString(getPaths(testName, source, listName))
    val list_target = readSql(spark, getPaths(testName, target, listName))
      .drop("comment")
      .collect().map(x => x.toSeq.toList.mkString(";")).toList
    var chek = true

    try {
      if (list_source.length == list_target.length) {
        list_source.foreach(x => {
          if (!list_target.contains(x)) {
            chek = false
          }
        })
        try {
          if (!chek)
            throw new Exception(s"The source and the showcase do not match in test $testName")
        }
        catch {
          case exception: Exception => println(exception)
        }
      }
      else throw new Exception(s"Different table sizes in test $testName")
    }
    catch {
      case exception: Exception => println(exception)
    }
  }

  def chekCounts(testName: String, listName: List[String]): Unit = {
    val test_source = readSql(spark, getPaths(testName, source, listName))
    val test_target = readSql(spark, getPaths(testName, target, listName))

    try {
      if (test_source.head()(0).toString != test_target.head()(0).toString)
        throw new Exception(s"The number of rows does not match in test $testName")
    }
    catch {
      case exception: Exception => println(exception)
    }
  }

  def chekArrays(testName: String, listName: List[String]): Unit = {
    val test_source = readSql(spark, getPaths(testName, source, listName))
    val test_target = readSql(spark, getPaths(testName, target, listName))

    try {
      if (test_source.except(test_target).count() != 0
        && test_target.except(test_source).count() != 0)
        throw new Exception(s"Attributes don't match in test $testName")
    }
    catch {
      case exception: Exception => println(exception)
    }
  }

  // метод для поиска пути к тесту по номеру теста, типу(source илии target) и определенному списку  
  
  def getPaths(numberTest: String, nameRec: String, pathList: List[String]): String = {
    var pathName = ""
    pathList.foreach(path => {
      if (path.contains(numberTest)) {
        if (path.contains(nameRec)) {
          pathName = path
        }
      }
    })
    pathName
  }

  // метод для получения таблицы из sql-запроса в тестовом фале
  
  def readSql(spark: SparkSession, filePath: String): DataFrame = {
    spark.sql(scala.io.Source.fromInputStream(getClass.getResourceAsStream(filePath))
      .getLines().mkString(""))
  }

  // метод для прочтения данных из теста и запись в список
  
  def readString(filePath: String): List[String] = {
    scala.io.Source.fromInputStream(getClass.getResourceAsStream(filePath))
      .getLines().mkString("|")
      .split("\\|").toList
  }
}

