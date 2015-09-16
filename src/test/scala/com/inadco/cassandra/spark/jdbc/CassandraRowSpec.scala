package com.inadco.cassandra.spark.jdbc

import com.datastax.spark.connector.types.TextType

import collection.mutable.Stack
import org.scalatest._

class CassandraRowSuite extends FlatSpec with Matchers {

  "Conversion from joda time to sql" should "works for empty or non empty values" in {
    val utils = new CassandraRowUtils()
    
    utils.getSqlDate(None) should be (null)
    
    val jodaTime = Some(new org.joda.time.DateTime(1428968606000L))
    
    val sqlTime = utils.getSqlDate(jodaTime)
    sqlTime should not be null
    sqlTime.getMinutes() should be (43)
    
  }

  it should "throw NoSuchElementException if data type is not supported" in {
    val emptyStack = new Stack[Int]
    a [RuntimeException] should be thrownBy {
      val utils = new CassandraRowUtils()
      
      utils.extractCassandraRowValue(null, ("testCol",TextType))
    } 
  }
}