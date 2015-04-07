# cassandra-spark-jdbc-bridge
If you want to query Cassandra data via JDBC and want to use Spark SQL to process the data so you can do things that Cassandra doesn't support yet like grouping or joining, you need this library.

This is a spark app which automatically brings all Cassandra tables into Spark as schema RDDs. Then it starts an embedded Apache HiveThriftServer to make the schema RDDs ready to be consumed via jdbc:hive2 protocol.
So you can use any BI tool like Jasper or Pentaho or Tableau to connect to Spark via JDBC and use the power of Spark SQL to process and query Cassandra tables.
    
