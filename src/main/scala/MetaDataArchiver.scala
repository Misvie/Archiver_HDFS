import java.io.PrintWriter
import java.sql.{ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.vertica.spark.rdd.VerticaRDD
import org.apache
import org.apache.spark
import org.apache.spark.sql.types.{DataTypes, DateType}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions._

class MetaDataArchiver(master: String=null) {

  var conf = new SparkConf()
    .setAppName("AGS ApiMetaData")

  //if (master!=null)
  //  conf.setMaster("local[1]")



  val spark = SparkSession.builder().config(conf).getOrCreate()

  val table = "ApiMetaData"
  val db = "LuckyPlay"
  val user = "dbadmin"
  val password = "Park3r20994"
  val host = "172.30.3.136"
  val port = 5433
  val schema = "LuckyPlayCasino"
  val opt = Map("host" -> host, "table" -> table, "db" -> db, "user" -> user, "password" -> password,"dbschema"->schema)
  val df = spark.read.format("com.vertica.spark.datasource.DefaultSource").options(opt).load()
  var sqlfunc = spark.udf.register("ToWWYear",MetaDataArchiver.ToWWYear _);

  var newschema = df.withColumn("wwyear",sqlfunc(df("TransactionDay")))
  newschema.createOrReplaceTempView(table);
  newschema.printSchema()
  var prevwwStartDate = Calendar.getInstance().getTime()
  var prevww = GetPrevWWYear(Calendar.getInstance().getTime())


  def ArchiveRange(startDate:Timestamp,endDate:Timestamp) ={
    var filtered  = spark.sql("select * from %s".format(table )).filter(newschema("TransactionDay") >=  startDate and newschema("TransactionDay")  < endDate)
    //filtered.write.mode(SaveMode.Append).partitionBy("wwyear").parquet("/AGSi/ApiMetaData")
  }
  def Archive() = {
    var cal = Calendar.getInstance()
    cal.setTime(prevwwStartDate)
    cal.add(Calendar.DATE,7)
    ArchiveRange(new Timestamp(prevwwStartDate.getTime),new Timestamp(cal.getTime.getTime))
  }

  def StartDateOfCurrentWW(): Date = {


    var cal = java.util.Calendar.getInstance();
    cal.set(Calendar.DAY_OF_WEEK,Calendar.MONDAY);
    cal.add(Calendar.DATE,-7)
    return cal.getTime
  }

  def GetPrevWWYear(date:Date) : Int ={
    var cal = java.util.Calendar.getInstance();
    cal.setTime(date)
    var ww = cal.get(Calendar.WEEK_OF_YEAR)
    var prevww = ww;
    while (prevww== ww)
    {
      cal.add(Calendar.DATE,-1);
      prevww = cal.get(Calendar.WEEK_OF_YEAR);
    }
    var year =  cal.get(Calendar.YEAR)
    cal.add(Calendar.DATE,-6);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    prevwwStartDate = cal.getTime()
    return year*100+prevww
  }
}

object MetaDataArchiver extends Serializable {
  val sampleDate = new SimpleDateFormat("yyyy-MM-dd")
  def ToWWYear(date :Date): Int = {
    var cal = java.util.Calendar.getInstance();
    cal.setTime(date)
    var ww = cal.get(Calendar.WEEK_OF_YEAR)
    var year =  cal.get(Calendar.YEAR)
    return year*100+ww
  }

}




