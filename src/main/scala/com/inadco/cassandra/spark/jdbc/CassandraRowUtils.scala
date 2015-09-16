package com.inadco.cassandra.spark.jdbc

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.types._
import org.apache.spark.sql.cassandra.{ColumnDataType, CassandraRelation}
import scala.collection.mutable._
import org.apache.spark.Logging
import java.sql.Timestamp
import org.joda.time.DateTime
import org.apache.spark.Logging

/**
 * Util class to deal with Cassandra rows
 * @author hduong
 */
class CassandraRowUtils extends Serializable with Logging{
	/**
	 * Extract a value of a column from a Cassandra row based on its data type
	 * Currently only support basic/primitive data types
	 */

	def extractCassandraRowValue(row: CassandraRow, colMeta: (String, ColumnType[_])): Any = {
		
		val colName = colMeta._1;

		var cassandraDataType = colMeta._2
		//reference this link for all the data types in Cassandra
		//http://grepcode.com/file/repo1.maven.org/maven2/org.apache.cassandra/cassandra-all/1.1.0/org/apache/cassandra/db/marshal/
		cassandraDataType match {
			case TextType | AsciiType | VarCharType => {
				val v = row.getStringOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case BooleanType => {
				val v = row.getBooleanOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case IntType => return {
				val v = row.getIntOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case BigIntType => return {
				val v = row.getLongOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case CounterType => {
				val v = row.getLongOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case BlobType => {
				val v = row.getBytesOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case VarIntType => {
				val v = row.getVarIntOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case DecimalType => {
				val v = row.getDecimalOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case DoubleType => {
				val v = row.getDoubleOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case FloatType => {
				val v = row.getFloatOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case InetType => {
				val v = row.getInetOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case UUIDType => {
				val v = row.getUUIDOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case TimeUUIDType => {
				val v = row.getUUIDOption(colName)
				if(v.isEmpty){
					return null;
				}else{
					return v.get
				}
			}
			case TimestampType => {
				val v = row.getDateTimeOption(colName)
				return getSqlDate(v)
			}

			case SetType(et)  => {
				//TODO: implement
					return null;
			}

			case ListType(et) => {
				//TODO: implement
				return null;
			}

			case MapType(kt, vt) => {
				//TODO: implement
				return null;
			}

			case UserDefinedType(_, fields) => return {
				//TODO: implement
				return null;
			}

			case _ => throw new RuntimeException("Column " + colName + " has unsupported data type " + cassandraDataType)
		}
	}
	
	/**
	 * Convert joda datetime into sql datetime
	 */
	def getSqlDate(v: Option[DateTime]): java.sql.Timestamp={
		if(v.isEmpty){
			return null;
		}else{			
			return new java.sql.Timestamp(v.get.getMillis)
		}		
	}
	/**
	 * Map a Cassandra row into a Spark sql row. To be used to construct a RDD
	 */
	def convertToSqlRow (row: CassandraRow, colList: Array[(String, ColumnType[_])]): org.apache.spark.sql.Row = {
		return org.apache.spark.sql.Row.fromSeq(colList.map(colMeta =>
    				extractCassandraRowValue(row, colMeta))) 
    				
	}
}
