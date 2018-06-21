import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache
import org.apache.spark
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


class Repartioner(tablename: String,datecolumnName: String,master: String=null) {
  val conf = new SparkConf()

  if (master!=null)
    conf.setMaster(master)
      .setAppName("AGS Archiver")


  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

val md = spark.read.parquet("hdfs://54.205.189.204/luckyplay/ApiMetaData");
  md.printSchema()
  val df = spark.read.parquet("/luckyplay/transactions");
  var sqlfunc = spark.udf.register("ToWWYear",Repartioner.ToWWYear _);
  var newschema = df.withColumn("wwyear",sqlfunc((df("TransactionDate"))))
  newschema = newschema.withColumn("Tdate",(df("TransactionDate").cast("timestamp")))
  newschema = newschema.withColumn("Udate",(df("UpdateDate").cast("timestamp")))
  newschema = newschema.withColumn("tday",(df("TransactionDay").cast("timestamp")))
  newschema = newschema.withColumn("schips",df("SafeChips").cast("decimal(38,2)"))
  newschema = newschema.withColumn("cgxp",df("CurrentGameXP").cast("decimal(38,2)"))
  newschema = newschema.withColumn("gxp",df("GameExperiencePoints").cast("decimal(38,2)"))
  newschema = newschema.withColumn("csafe",df("CurrentSafe").cast("decimal(38,2)"))
  newschema = newschema.withColumn("csafebalance",df("CurrentSafeBalance").cast("decimal(38,2)"))
  newschema = newschema.withColumn("ex4",df("ExtraData4").cast("decimal(38,2)"))



    newschema.createOrReplaceTempView(tablename);
  newschema.printSchema()
  def Archive() ={
     var filtered  = spark.sql("SELECT ID, UserID, ContextNode1ID, ContextNode2ID, IP, Tdate as TransactionDate, StatusID, Cash, Coins, ExperiencePoints, SubTypeID, TypeID, ExtraData, UDate as UpdateDate, CurrentBalance, CurrentXP, ExtraData2, ExtraData3, CreatedWhere, CurrentUserDeviceId, tday as TransactionDay, RoomID, Credits, Points, CurrentPoints, CurrentCredits, GameTypeID, GameSetupID, FtueLevel, FRC,cgxp as CurrentGameXP,gxp as  GameExperiencePoints, ExtraTextData, LiveEventTickets, CurrentLiveEventTickets, CurrentFppPoints, FppPoints, CurrentMegaWheelSpins, MegaWheelSpinsBought,csafe as CurrentSafe,csafebalance as  CurrentSafeBalance,schips as  SafeChips,ex4 as  ExtraData4, AppId,wwyear from %s ".format(tablename));
    filtered.printSchema()
     filtered.write.mode(SaveMode.Append).partitionBy("wwyear").parquet("/AGSi/transactions")
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
    return year*100+ww
  }
}

object Repartioner extends Serializable {
  val sampleDate = new SimpleDateFormat("yyyy-MM-dd")

  def ToWWYear(date :String): Int = {

        var cal = java.util.Calendar.getInstance();
        cal.setTime(sampleDate.parse(date))
        var ww = cal.get(Calendar.WEEK_OF_YEAR)
        var year =  cal.get(Calendar.YEAR)
        return year*100+ww


  }

}



