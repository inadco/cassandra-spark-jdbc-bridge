package com.inadco.cassandra.spark.jdbc

import com.datastax.spark.connector.types.ColumnType
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.{ColumnDataType, CassandraRelation}
/**
 * Util class to deal with hive schemas
 * @author hduong
 */
object HiveSchemaUtils {
	/**
	 * Map a column in Cassandra to a column to be defined in schema RDD
	 * Currently only support basic/primitive data types.
	 */
	def createStructField (colMeta: (String, ColumnType[_])) : StructField = {
		val colName = colMeta._1;
		val	columnType: DataType = ColumnDataType.catalystDataType(colMeta._2, true)
		StructField(colName, columnType, true)
	}
	
	/**
	 * Check to see if two schemas (of RDDs) are the same
	 */
	def isSameSchema(hiveSchema1: Option[StructType], hiveSchema2: Option[StructType]): Boolean = {
		if(hiveSchema1.isEmpty && hiveSchema2.isEmpty){
			return true;
		}else if (hiveSchema1.isEmpty || hiveSchema2.isEmpty){
			return false;
		}else{
			return hiveSchema1.get.treeString.equals(hiveSchema2.get.treeString)
		}		
	}
}
