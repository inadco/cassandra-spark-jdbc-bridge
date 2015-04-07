package com.inadco.cassandra.spark.jdbc

import com.datastax.spark.connector.CassandraRow
import scala.collection.mutable._
import org.apache.spark.Logging
import java.sql.Timestamp
import org.joda.time.DateTime
import org.apache.spark.Logging

/**
 * @author hduong
 */
class CassandraRowUtils extends Serializable with Logging{
	
	def extractCassandraRowValue(row: CassandraRow, colMeta: (String, String)): Any = {	
		
		val colName = colMeta._1;
		
		var cassandraDataType = colMeta._2
		//reference this link for all the data types in Cassandra
		//http://grepcode.com/file/repo1.maven.org/maven2/org.apache.cassandra/cassandra-all/1.1.0/org/apache/cassandra/db/marshal/
		cassandraDataType match {
			case "org.apache.cassandra.db.marshal.AsciiType" => {
				val v = row.getStringOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.BooleanType" => {
				val v = row.getBooleanOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.BytesType" => return {
				val v = row.getByteOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.CounterColumnType" => {
				val v = row.getLongOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.DateType" => {
				val v = row.getDateOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.DecimalType" => {
				val v = row.getDecimalOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.DoubleType" => {
				val v = row.getDoubleOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.LongType" => {
				val v = row.getLongOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
      }
			case "org.apache.cassandra.db.marshal.FloatType" => {				
				val v = row.getFloatOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.Int32Type" => {
				val v = row.getIntOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.IntegerType" => {
				val v = row.getIntOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.LexicalUUIDType" => {
				val v = row.getUUIDOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.UTF8Type" => return {
				val v = row.getStringOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			
			case "org.apache.cassandra.db.marshal.UUIDType" => {
				val v = row.getUUIDOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case "org.apache.cassandra.db.marshal.TimestampType" => {				
				val v = row.getDateTimeOption(colName)
				return getSqlDate(v)
			}
			case "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)" => {
				val v = row.getDateTimeOption(colName)
				return getSqlDate(v)
			}
			case _ => throw new RuntimeException("Column " + colName + " has unsupported data type " + cassandraDataType)
		}
	}
	def getSqlDate(v: Option[DateTime]): java.sql.Timestamp={
		if(v.isEmpty){
			return null;
		}else{			
			return new java.sql.Timestamp(v.get.getMillis)
		}		
	}
	
	def convertToSqlRow (row: CassandraRow, colList: Array[(String, String)]): org.apache.spark.sql.Row = {		
		return org.apache.spark.sql.Row.fromSeq(colList.map(colMeta =>
    				extractCassandraRowValue(row, colMeta))) 
    				
	}
}
