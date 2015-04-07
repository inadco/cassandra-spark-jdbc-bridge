package com.inadco.cassandra.spark.jdbc

import org.apache.spark.sql._
import com.datastax.spark.connector.types.UUIDType
/**
 * @author hduong
 */
object HiveSchemaUtils {
	def createStructField (colMeta: (String, String)) : StructField = {
		var dataType: DataType = StringType
		val colName = colMeta._1;
		
		var cassandraDataType = colMeta._2
		//reference this link for all the data types in Cassandra
		//http://grepcode.com/file/repo1.maven.org/maven2/org.apache.cassandra/cassandra-all/1.1.0/org/apache/cassandra/db/marshal/
		cassandraDataType match {
			case "org.apache.cassandra.db.marshal.AsciiType" => dataType = StringType
			case "org.apache.cassandra.db.marshal.BooleanType" => dataType = BooleanType
			case "org.apache.cassandra.db.marshal.BytesType" => dataType = ByteType
			case "org.apache.cassandra.db.marshal.CounterColumnType" => dataType = LongType
			case "org.apache.cassandra.db.marshal.DateType" => dataType = DateType
			case "org.apache.cassandra.db.marshal.DecimalType" => dataType = FloatType
			case "org.apache.cassandra.db.marshal.DoubleType" => dataType = DoubleType
			case "org.apache.cassandra.db.marshal.LongType" => dataType = LongType
			case "org.apache.cassandra.db.marshal.FloatType" => dataType = FloatType
			case "org.apache.cassandra.db.marshal.Int32Type" => dataType = IntegerType
			case "org.apache.cassandra.db.marshal.IntegerType" => dataType = IntegerType
			case "org.apache.cassandra.db.marshal.LexicalUUIDType" => dataType = StringType
			case "org.apache.cassandra.db.marshal.UTF8Type" => dataType = StringType
			
			case "org.apache.cassandra.db.marshal.UUIDType" => dataType = StringType
			case "org.apache.cassandra.db.marshal.TimestampType" => dataType = TimestampType
			
			case "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)" => dataType = TimestampType
			
			
			case _ => throw new RuntimeException("Column " + colName + " has unsupported data type " + cassandraDataType)
		}
		StructField(colMeta._1, dataType, true)
	}
	
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
