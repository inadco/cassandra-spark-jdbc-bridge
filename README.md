# cassandra-spark-jdbc-bridge
If you want to query Cassandra data via JDBC and want to use Spark SQL to process the data so you can do things that Cassandra doesn't support yet like grouping or joining, you need this library.

This is a spark app which automatically bring all Cassandra tables into Spark as schema RDDs, then register those schema RDDs to HiveContext, then it starts an embedded Apache HiveThriftServer to make the RDDs ready to be consumed via jdbc:hive2 protocol.
So you can use any BI tool like Jasper or Pentaho or Tableau to query Cassandra tables via JDBC and use the power of Spark SQL to process the data.   
