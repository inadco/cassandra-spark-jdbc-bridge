

# cassandra-spark-jdbc-bridge
If you want to query Cassandra data via JDBC and but you want to use the power of Spark SQL to do data processing, you need this application.

This app (CSJB), which is a Spark app, will automatically registers all Cassandra tables as schema RDDs in Spark SQL and starts an embedded Apache HiveThriftServer to make the RDDs ready to be consumed via "jdbc:hive2" protocol.

With this bridge/server, you can use any BI tool like Jasper, Pentaho or Tableau to query Cassandra tables via Spark SQL. You will be able to do many different aggregation and/or filtering operations (like group by, join..) with Cassandra data.

This application uses the Datastax's [spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector) 

Notes: 

 - Cassandra tables are registered in Spark SQL using naming format below:
**keyspace_tableName**
Example: "mykeyspace.test" table in Cassandra will be registered as "mykeyspace_mytable" schema RDD.

 - As of now, CSJB only supports Cassandra tables with the following data types: `AsciiType, BooleanType, BytesType,CounterColumnType, DateType, DecimalType, DoubleType, LongType, FloatType, Int32Type, IntegerType, LexicalUUIDType, UTF8Type, UUIDType, TimestampType, ReversedType (for TimestampType)`. More data types will be added in the near future (your contribution will be greatly appreciated). 

=============
Getting started

**Prerequisites**
 - Install and start Cassandra 2.1
 - Install and start Spark 1.2
 
**Build**

    sbt assembly copyAssets

 A fat assembly jar and config files will be generated in "target/dist"
 
 **Set environment variables** 

    export INADCO_CSJB_HOME=`pwd`/target/dist
    export SPARK_HOME=YOUR_SPARK_HOME_DIR

 **Enter Cassandra credentials and Spark master url**
 
 Default config values are in $INADCO_CSJB_HOME/dist/config/csjb-default.properties

    spark.cassandra.connection.host=localhost
    spark.cassandra.auth.username=CASSSANDRA_USER_NAME
    spark.cassandra.auth.password=CASSSANDRA_PASSWORD
    inadco.spark.master=local[2]
    
**Start CSJB server** 

    $INADCO_CSJB_HOME/bin/start.sh

The CSJB server is started at port 10000 (to stop it, run $INADCO_CSJB_HOME/bin/stop.sh)
Status of CSJCB app is available via your Spark UI http://YOUR_SPARK_MASTER:8080/
    
**Query your data**

Run a client which supports "jdbc:hive2" protocol (e.g "beeline") and connect your CSJB server to query existing data in Cassandra (if you don't have the data yet, run cql and create a test table and insert some data in Cassandra first)

    $SPARK_HOME/bin/beeline
    beeline> !connect jdbc:hive2://localhost:10000
    0: jdbc:hive2://localhost:10000> select id, sum(count) from mykeyspace_mytable group by id;