// Databricks notebook source
// loading dependencies 
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
//import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.Column
import sqlContext.implicits._
//import com.aemo.ctrlfwk.ControlFwk

// COMMAND ----------

// Getting the schema from the configuration files

import java.io.File
import com.typesafe.config.{Config,ConfigFactory }
val confFile:File = new File("/dbfs/mnt/catsadls/Config/CATSConfiguration.conf")
val config:Config = ConfigFactory.parseFile(confFile).resolve

var CND_Schema = new StructType()
val cols = config.getString("cnd_columns")
val columnsList = cols.split(",")
columnsList.foreach(x => { CND_Schema = CND_Schema.add(x, StringType, true)})

var CND_ADLSPath = config.getString("CND_ADLS_PATH")
val SYS_PROC_ID_CND = 1006
val STRM_NAME_ADLS_COSMOS = "adls-cosmos"


// COMMAND ----------

//This block
//1. Define the Function "transformIngestBatch" to transform and ingest the csv data for each batch
// 1.1. The function reads the CND History files as dataframes
// 1.2 Then it selects the nmi, postcode, customerclassification, nmiclasscode columns from the dataframe table based on highend date and Active record
// 1.3 It then calculate the Affected count
// 1.4 Write the result to Cosmos


def transformIngestBatch(batchName:String, batchPath:String, cosmosConfigWrite:com.microsoft.azure.cosmosdb.spark.config.Config):Long = {
  
 var read_CND:org.apache.spark.sql.DataFrame = null
  

  if( !dbutils.fs.ls(batchPath).map(x=>x.name).filter(x=>x.contains("CATS_CATS_NMI_DATA")).isEmpty)
  read_CND = sqlContext.read.format("csv").schema(CND_Schema).option("delimiter", "\u0010").load(batchPath + "CATS_CATS_NMI_DATA*.csv")
  
  //read_CND = sqlContext.read.format("csv").schema(CND_Schema).load(batchPath + "CATS_CATS_NMI_DATA*.csv")
  
  val CND_Table = read_CND.createOrReplaceTempView("CND")
   
  val CND_Query = spark.sqlContext.sql("select nmi as id, nmi as prtn_key, nmi, postcode, cnd_01 as customerclassification, nmiclasscode from CND where enddate like '9999-12-31%' and maintactflg='A' ")
  
  val CND_Updated_Table = CND_Query.createOrReplaceTempView("UPDATEDCND")  
  val CND_Updated_Query = spark.sqlContext.sql("select * from UPDATEDCND")    
  val AfftdCount = CND_Updated_Query.count  
  
  spark.sqlContext.sql("drop table CND")
        
  CND_Updated_Query.write.mode(SaveMode.Append).cosmosDB(cosmosConfigWrite)
   
  return AfftdCount
 }



/*
def transformIngestBatch(adlsretailerPath:String, cosmosConfig:com.microsoft.azure.cosmosdb.spark.config.Config):Long = {
  
    var read_Retailer:org.apache.spark.sql.DataFrame = null
    
  read_Retailer = sqlContext.read.format("csv").schema(Retailer_Schema).load(adlsretailerPath + "Retailer*.csv")  
  val retailer_table = read_Retailer.createOrReplaceTempView("retailer")
  val retailer_query = spark.sqlContext.sql("select 'Retailer' as id,'active' as status,collect_list(struct(participantid as frmp_participantid, participantname,retailernameid)) as Participants from retailer group by id,status")
  val retailer_Query_Count = retailer_query.count
    retailer_query.write.mode(SaveMode.Append).cosmosDB(cosmosConfig)  
   return retailer_Query_Count
}*/

// COMMAND ----------

val configMap = Map(
  "Endpoint" -> {config.getString("COSMOS_END_POINT")},
  "Masterkey" -> dbutils.secrets.get(scope=config.getString("COSMOS_SCOPE"),key=config.getString("COSMOS_KEY")),
  "Database" -> {config.getString("COSMOS_DB_NAME")},
  "Collection" -> {config.getString("COSMOS_COLLECTION_NAME")},
  "preferredRegions" -> {"Australia East"},
  "Upsert" -> {"true"})

var cosmosConfigWrite = com.microsoft.azure.cosmosdb.spark.config.Config(configMap)

// COMMAND ----------

//This block
//2. Define the Cosmos configuration
//3. Call the transformIngestBatch function for each batches
//4. rowCnt gives the Affected Count value

val configMap = Map(
  "Endpoint" -> {config.getString("COSMOS_END_POINT")},
  "Masterkey" -> dbutils.secrets.get(scope=config.getString("COSMOS_SCOPE"),key=config.getString("COSMOS_KEY")),
  "Database" -> {config.getString("COSMOS_DB_NAME")},
  "Collection" -> {config.getString("COSMOS_COLLECTION_NAME")},
  "preferredRegions" -> {"Australia East"},
  "Upsert" -> {"true"})

var cosmosConfigWrite = com.microsoft.azure.cosmosdb.spark.config.Config(configMap)

//Get the list of CND batches in a sorted order
val CNDBatchList:Seq[String] = dbutils.fs.ls(CND_ADLSPath).map(x=>x.name).sorted

//Process all the CND batches
CNDBatchList.foreach(x=> {
   //ControlFwk.startStream(x,SYS_PROC_ID_CND,STRM_NAME_ADLS_COSMOS)
   val batchPath = CND_ADLSPath+x    
    //Call a function to ingest this batch  
    val rowCnt = transformIngestBatch(x, batchPath, cosmosConfigWrite)
   // ControlFwk.stopStream(x,SYS_PROC_ID_CND,STRM_NAME_ADLS_COSMOS, rowCnt)
 })



// COMMAND ----------

