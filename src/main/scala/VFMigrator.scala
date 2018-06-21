import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

class VFMigrator {
  var conf = new SparkConf()
    .setAppName("VF Migrator")

  //conf.setMaster("local[1]")

  val spark = SparkSession.builder().config(conf).getOrCreate()

  val table = "Transaction"
  val db = "LuckyPlay"
  val user = "dbadmin"
  val password = "Park3r20994"
  val host = "172.30.3.136"
  val port = 5433
  val schema = "VegasFever"
  val opt = Map("host" -> host, "table" -> table, "db" -> db, "user" -> user, "password" -> password,"dbschema"->schema)
  val df = spark.read.format("com.vertica.spark.datasource.DefaultSource").options(opt).load()
  var sqlfunc = spark.udf.register("ToWWYear",Archiver.ToWWYear _);

  var newschema = df.withColumn("wwyear",sqlfunc(df("TransactionDay")))
  newschema.createOrReplaceTempView(table);
  newschema.printSchema()
  var prevwwStartDate = Calendar.getInstance().getTime()


  def ArchiveRange(startDate:Timestamp,endDate:Timestamp) ={
    var filtered  = spark.sql("select * from %s".format(table ))//.filter(newschema("TransactionDay") >=  startDate and newschema("TransactionDay")  < endDate)
    filtered.write.mode(SaveMode.Append).partitionBy("wwyear").parquet("/AGSi/VF/Transactions")
  }
  def Archive() = {
    var cal = Calendar.getInstance()
    cal.setTime(prevwwStartDate)
    cal.add(Calendar.DATE,7)
    ArchiveRange(new Timestamp(prevwwStartDate.getTime),new Timestamp(cal.getTime.getTime))
  }





}
