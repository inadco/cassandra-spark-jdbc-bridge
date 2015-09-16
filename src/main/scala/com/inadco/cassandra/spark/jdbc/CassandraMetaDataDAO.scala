package com.inadco.cassandra.spark.jdbc

import java.util._
import com.datastax.driver.core.Row
import com.datastax.driver.core._
import com.datastax.spark.connector.types.ColumnType
import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}
import org.apache.spark.sql._

/**
 * DAO class to extract metadata of all the tables available in Cassandra
 * @author kmathur
 */
class CassandraMetaDataDAO (conf : SparkConf){
	val schemaObjects: collection.mutable.Map[String, collection.mutable.Map[String, collection.mutable.Map[String, ColumnType[_]]]] = init(conf)

	def init(conf: SparkConf) : collection.mutable.Map[String, collection.mutable.Map[String, collection.mutable.Map[String, ColumnType[_]]]] = {

		val keyspaces = collection.mutable.Map[String, collection.mutable.Map[String, collection.mutable.Map[String, ColumnType[_]]]]()

		Schema.fromCassandra(CassandraConnector(conf)).keyspaces.foreach { keyspace =>
			if (keyspace.keyspaceName != "system" && keyspace.keyspaceName.indexOf("system_") != 0) {

				val tables = collection.mutable.Map[String, collection.mutable.Map[String, ColumnType[_]]]()

				keyspace.tables.foreach { table =>

					val columns = collection.mutable.Map[String, ColumnType[_]]()
					table.allColumns.foreach { column =>
						columns.put(column.columnName, column.columnType)
					}

					tables.put(table.tableName, columns)
				}

				keyspaces.put(keyspace.keyspaceName, tables)
			}

		}
		return keyspaces
	}
	/**
	 * Get list of all keyspaces excluding system keyspace
	 */
	def getKeySpaceList() : scala.collection.mutable.Set[String]  = {
		if (schemaObjects == null) {
			return null
		}
		val keyspaceList = scala.collection.mutable.Set[java.lang.String]()

		schemaObjects.keys.foreach { keyspace =>
			keyspaceList += keyspace
		}
		return keyspaceList
	}
	/**
	 * Get a list of all tables by a keyspace
	 */
	def getTableList(keyspace: String) : scala.collection.mutable.Set[String]  = {
		if (schemaObjects == null || keyspace == null) {
			return null
		}
		val tableList = scala.collection.mutable.Set[java.lang.String]()

		val tables = schemaObjects.get(keyspace).getOrElse(null)
		if (tables == null) {
			return null
		}
		tables.keys.foreach { table =>
			tableList += table
		}
		return tableList
	}
	/**
	 * Get a list of all columns of a table
	 */
	def getTableColumns(keyspace: String, table: String) : collection.mutable.Map[String, ColumnType[_]] = {
		if (schemaObjects == null || keyspace == null || table == null) {
			return null
		}
		val columnList = collection.mutable.Map[String, ColumnType[_]]()

		val tables = schemaObjects.get(keyspace).getOrElse(null)
		if (tables == null) {
			return null
		}

		val columns = tables.get(table).getOrElse(null)
		if (columns == null) {
			return null
		}
		columns.foreach { case (column:String, types:ColumnType[_]) =>
			columnList.put(column, types);

		}
		return columnList
	}
}
