// Databricks notebook source
val CNDBatchList:Seq[String] = dbutils.fs.ls("/").map(x=>x.name).sorted

// COMMAND ----------

CNDBatchList.foreach(x=> {
   //ControlFwk.startStream(x,SYS_PROC_ID_CND,STRM_NAME_ADLS_COSMOS)
   val batchPath = "ppp"+x    
    //Call a function to ingest this batch  
    val rowCnt = transformIngestBatch(x, batchPath, cosmosConfigWrite)
   // ControlFwk.stopStream(x,SYS_PROC_ID_CND,STRM_NAME_ADLS_COSMOS, rowCnt)
 })

// COMMAND ----------

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

def transformIngestBatch(batchName:String, batchPath:String):Long = {
  
 print(batchPath)
   return 11L
 }

// COMMAND ----------

CNDBatchList.foreach(x=> {
   //ControlFwk.startStream(x,SYS_PROC_ID_CND,STRM_NAME_ADLS_COSMOS)
   val batchPath = "ppp"+x    
    //Call a function to ingest this batch  
    val rowCnt = transformIngestBatch(x, batchPath)
   // ControlFwk.stopStream(x,SYS_PROC_ID_CND,STRM_NAME_ADLS_COSMOS, rowCnt)
 })

// COMMAND ----------

