import java.sql.{ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf

object Program {


  def main(args: Array[String]) : Unit =
  {
//    var a = new Archiver("Transaction","TransactionDay","local[1]",Array("C:\\Users\\Administrator\\Downloads\\vertica-jdbc-7.2.3-0.jar", "C:\\Users\\Administrator\\Downloads\\vertica-8.1.1_spark2.1_scala2.11-20170623.jar"))
//    a.Archive();
    var a = new Repartioner("Transaction","TransactionDay","local[1]")
    a.Archive();
  }
  /*def main1(args: Array[String]) {

    // initialise spark context


   // var tt = a.StartDateOfCurrentWW()
    //var a = ToWWYear(sampleDate.parse("2017-01-27"))

    val conf = new SparkConf()
      .setAppName("HelloWorld")
      .setMaster("local[1]")
      .setJars( Array("C:\\Users\\Administrator\\Downloads\\vertica-jdbc-7.2.3-0.jar", "C:\\Users\\Administrator\\Downloads\\vertica-8.1.1_spark2.1_scala2.11-20170623.jar"))


    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      .getOrCreate()


    spark.udf.register("ToWWYear",ToWWYear _);


var gg = spark.read.parquet("hdfs://172.30.3.185/staging/RequestMetaData");
    // terminate spark context
    val table = "Transaction"
    val db = "LuckyPlay"
    val user = "dbadmin"
    val password = "Park3r20994"
    val host = "172.30.3.136"
    val schema = "SCStaging"

    val now = Calendar.getInstance()

    val part=12
    val opt = Map("host" -> host, "table" -> table, "db" -> db, "user" -> user, "password" -> password,"dbschema"->schema)
    val df = spark.read.format("com.vertica.spark.datasource.DefaultSource").options(opt).load()
    df.createOrReplaceTempView("tran");
    val bb = spark.sql("select * from tran where ToWWYear(TransactionDay) = 201736")
//       val prop = new Properties
//            prop.put("user", "dbadmin")
//            prop.put("password", "Park3r20994")
//        val extractValues = (r: ResultSet) => {
//          ( r.getTimestamp("TransactionDay"),r.getLong("ID"))
//
//        }
//            val cols = Array[String]()
//        val port = 5433
//            val data = VerticaRDD.create(sc, host, port, db, prop, table, cols, numPartitions = part, mapRow = extractValues)
  // var dd = df.filter(r=> r.getAs[Timestamp]("TransactionDay").compareTo(sampleDate.parse("2017-08-10")) >0 )

    //dd.write.parquet("hdfs://172.30.3.185/staging/Test");

   //  now.setTime(sampleDate.parse("2017-01-27"));
   // val currentHour = now.get(Calendar.WEEK_OF_YEAR)

   // var t = dateCompare("2017-08-27","2017-07-26")
   // var dd = data.filter(d=>d._1.compareTo(sampleDate.parse("2017-08-27")) > 0)
    val res = bb.count()

    println("count:" + res)

  }
  val sampleDate = new SimpleDateFormat("yyyy-MM-dd")

  //val sampleDate = new java.text.SimpleDateFormat("dd-MM-yyyy")

  def dateCompare(input:(String, String)): Boolean = {
    val date1 = sampleDate.parse(input._1)

    val date2 = sampleDate.parse(input._2)
    if (date1.compareTo(date2) > 0)  true
    else
      false
  }

  def ToWWYear(date :Date): Int = {

    var cal = java.util.Calendar.getInstance();
    cal.setTime(date)
    var ww = cal.get(Calendar.WEEK_OF_YEAR)
    var year =  cal.get(Calendar.YEAR)
    return year*100+ww


  }




*/
}

//General instructions:
//in order to run on spark have to build and delete out of jar files  : ip -d <jar file name>.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
//otherwise spark has security issues to run


object MainPartitioner{
  def main(args: Array[String]) :Unit =
  {
    var a = new Repartioner("Transaction","TransactionDay")
    a.Archive();
  }
}

object MainArchvier{
  def main(args: Array[String]) : Unit =
  {
    var a = new MetaDataArchiver()
    if (args.length == 0)
        a.Archive();
    else
      a.ArchiveRange(new Timestamp(Archiver.sampleDate.parse(args(0)).getTime),new Timestamp(Archiver.sampleDate.parse(args(1)).getTime));
  }
}


object MainVFMigrator{
  def main(args: Array[String]) : Unit =
  {
    var a = new VFMigrator()
    if (args.length == 0)
      a.Archive();
    else
      a.ArchiveRange(new Timestamp(Archiver.sampleDate.parse(args(0)).getTime),new Timestamp(Archiver.sampleDate.parse(args(1)).getTime));
  }




}


