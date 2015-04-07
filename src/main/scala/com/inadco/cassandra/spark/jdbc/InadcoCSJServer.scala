package com.inadco.cassandra.spark.jdbc

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import com.datastax.spark.connector._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import akka.actor._
import akka.actor.Scheduler
import akka.actor.Scheduler
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.tools.nsc.doc.model.Val

/**
 * @author hduong
 */
object InadcoCSJServer extends Logging {
	def main(args: Array[String]){
		try {
			val server = new InadcoCSJServer()
			server.init()
			server.start()
			logInfo("InadcoHiveThriftServer started")
		} catch {
			case e: Exception =>
				logError("Error starting InadcoHiveThriftServer", e)
			System.exit(-1)
		}
	}

}

class InadcoCSJServer extends Logging{
	val system = ActorSystem("System")
	val hiveTables = new scala.collection.mutable.HashMap[String, StructType]()
	def init(){
		
	}
	def start(){
		logInfo("Starting InadcoCSJServer")
		
		//load all the properties files
		val defaultConf = ConfigFactory.load();
		val overrideFile = new File(System.getenv("INADCO_CSJ_HOME") + "/config/override-application.properties")
		if(overrideFile.exists()){
			logInfo("Found override properties from: " + overrideFile.toString());
		}
		val conf = ConfigFactory.parseFile(overrideFile).withFallback(defaultConf);
		logInfo("spark.cassandra.connection.host is: " + conf.getString("spark.cassandra.connection.host"));
		logInfo("spark.cassandra.auth.username is: " + conf.getString("spark.cassandra.auth.username"));
		
		//init new spark context
		val sparkConf = new SparkConf()
		sparkConf.set("spark.cores.max", conf.getString("spark.cores.max"))
		sparkConf.set("spark.cassandra.connection.host",conf.getString("spark.cassandra.connection.host"));
		sparkConf.set("spark.cassandra.auth.username", conf.getString("spark.cassandra.auth.username"));
		sparkConf.set("spark.cassandra.auth.password", conf.getString("spark.cassandra.auth.password"));
		sparkConf.set("spark.executor.memory", conf.getString("spark.executor.memory"));
		
		
		sparkConf.setMaster(conf.getString("inadco.spark.master"));
		sparkConf.setAppName(conf.getString("inadco.appName"));
		val sc = new SparkContext(sparkConf)
		
		//add handler to gracefully shutdown
		Runtime.getRuntime.addShutdownHook(
			new Thread() {
	  		override def run() {
	      	logInfo("Shutting down InadcoHiveThriftServer...")
	        if(sc != null){
	        	sc.stop();
	        }
	      	logInfo("Spark context stopped.")
	  		}
      })
        
    //hive stuff
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
		HiveThriftServer2.startWithContext(hiveContext)
		
		//register all Cassandra tables		
		val startDelayMs = new FiniteDuration(0, java.util.concurrent.TimeUnit.MILLISECONDS)
		val intervalMs = new FiniteDuration(conf.getLong("inadco.tableList.refresh.intervalMs"), java.util.concurrent.TimeUnit.MILLISECONDS)

		val cancellable2 = system.scheduler.schedule(startDelayMs, intervalMs)({
			registerCassandraTables(sc, sparkConf, hiveContext)    	
		})		
		
		logInfo("InadcoCSJServer started successfully")
	}
	def stop(){
		
	}
	
	def registerCassandraTables(sc: SparkContext, sparkConf: SparkConf, hiveContext: HiveContext){
	  	var cassMetaDataDAO = new CassandraMetaDataDAO(sparkConf);
	  	val keyspaceList = cassMetaDataDAO.getKeySpaceList();
	  	keyspaceList.foreach{ keyspace =>  		  	
	  		cassMetaDataDAO.getTableList(keyspace).foreach(tableName => registerCassandraTable(keyspace, tableName, cassMetaDataDAO, sc, hiveContext))	
	  	}	  	
	}
	
	def registerCassandraTable(keyspace: String, tableName: String, cassMetaDataDAO: CassandraMetaDataDAO, sc: SparkContext, hiveContext: HiveContext){
	  	//TODO: try to use dot "." to have "dbName.tableName" in hive but it doesn't work when querying from beeline
	  	// looks like everything get registered to the "default" database in hive
	  	val hiveTableName = keyspace + "_" + tableName;
	  	try {
	  		val rdd = sc.cassandraTable(keyspace, tableName)
	  		val colList = cassMetaDataDAO.getTableColumns(keyspace, tableName).toArray
	  		val hiveSchema = StructType(colList.map(colMeta => HiveSchemaUtils.createStructField(colMeta)))
	  		
	  		val existingHiveSchema = hiveTables.get(hiveTableName);
	  		if(!HiveSchemaUtils.isSameSchema(existingHiveSchema, Some(hiveSchema))){	  			
		  		hiveTables.put(hiveTableName, hiveSchema);
		  		logInfo("Created hive schema " + hiveSchema.treeString)
		  		
		  		//broad cast column list to workers
		  		val cassRowUtils = sc.broadcast(new CassandraRowUtils());
		  		val broadCastedColList= sc.broadcast(colList)
		  		
		  		val rowRDD = rdd.map(
						row =>org.apache.spark.sql.Row.fromSeq(broadCastedColList.value.map(
            	colMeta =>cassRowUtils.value.extractCassandraRowValue(row, colMeta))))	  		
					
					val rowSchemaRDD = hiveContext.applySchema(rowRDD, hiveSchema)
				
					rowSchemaRDD.registerTempTable(hiveTableName)		
					logInfo("Registered table " + hiveTableName)	
	  		}
	  		
		} catch {
			case e: Exception => logError("Failed to handle table " + hiveTableName, e)
		}
	}
	
}



