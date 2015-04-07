package com.inadco.cassandra.spark.jdbc

import java.util._
import com.datastax.driver.core._
import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.CassandraConnector

class CassandraMetaDataDAO (conf : SparkConf){
	val rowsList : List[Row]=init(conf)
	val SCHEMA: String ="keyspace_name"
	val TABLE:  String ="columnfamily_name"
	val COLUMN: String ="column_name"
	val COLUMN_DATA_TYPE: String = "validator"


	def init(conf: SparkConf) : List[Row] = {
		val resultSetTables = CassandraConnector(conf).withSessionDo { 
			session => session.execute("select keyspace_name, columnfamily_name, column_name,validator from system.schema_columns")
		}
		return resultSetTables.all()
	}
	
	def getKeySpaceList() : scala.collection.mutable.Set[String]  = {
		if(rowsList==null){
			return null
		}
		val keyspaceList = scala.collection.mutable.Set[java.lang.String]()
		
		for(x <- 0 until rowsList.size()){
			var next=rowsList.get(x);
			//take keyspace excluding: "system", "system_*" 
			val ks = next.getString(SCHEMA)
			if(ks != "system" && ks.indexOf("system_") != 0){
				keyspaceList+=next.getString(SCHEMA)
			}			
		}
		return keyspaceList
	}
	
	def getTableList(keyspace: String) : scala.collection.mutable.Set[String]  = {
		if(rowsList==null){
			return null
		}
		val tables = scala.collection.mutable.Set[java.lang.String]()
		
		for(x <- 0 until rowsList.size()){
			var next=rowsList.get(x);
			if(next.getString(SCHEMA)==keyspace){
				tables+=next.getString(TABLE)
			}
		}
		return tables
	}
	
	def getTableColumns(schema:String,table:String) : collection.mutable.Map[String, String] = {
		if(rowsList==null){
			return null
		}
		val columns = collection.mutable.Map[String, String]()
		var x=0;
		for(x <- 0 until rowsList.size()){
			var next=rowsList.get(x);
			if(next.getString(SCHEMA)==schema && next.getString(TABLE)==table){
				columns.put(next.getString(COLUMN),next.getString(COLUMN_DATA_TYPE));
				}
			}
		return columns
	}
}
