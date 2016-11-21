# oracle-hadoop-importer
This project aims to import the data from Oracle database to Hadoop in an efficient way (better than Sqoop) and supports complex types. Import is done directly to a Parquet format.

## Usage
In order to run the importer you need to specify the following parameters
- jdbcURI - connection string to source database
- schema - database schema to be used for exporting (not necessery a table owner)
- password - password of a database schema to be user for data exporting
- sql - A query to be executed on the database in order to extract the data 
Optional parameters
- parallel - the number of parallel threads to be used in order to process JDBC outputs (default:1)
- batchSize - number of rows to be processed by a single thread during a single loop (default 1000)
- fetchSize - fetch size to be set on a JDBC driver
- type_map - comma separated list of column names to types mappings (needed for complex type importing, for basic column tapes importer will try to auto-detect)

Type Mapping Syntax: COLUMN_NAME:TYPE
- COLUMN_NAME - name of a column in a query result
- TYPE - internal type definition like [N#]INTERNAL_TYPE , where N# indicates is nullable

List of possible INTERNAL_TYPES:
- NUMBER -> maps to a double in parquet
- DECIMAL -> maps to a long in parquet
- CHAR -> maps to a string in parquet
- FLOAT -> maps to a float in parquet
- LONG -> maps to a long in parquet
- INT -> maps to a integer32 in parquet
- STRING  -> maps to a string in parquet
- TIMESTAMP -> maps to a double in parquet (up to nanosecond precision)
- TIMESTAMP_LONG -> maps to a long (milisecond precision)
- ARRAY(TYPE)


##Example 1
Importing an array of numbers
```
/java -cp ./target/OraDataImporter-1.0-SNAPSHOT.jar
-Dsql="select * from meetup.array_test" \
-DjdbcURI="jdbc:oracle:thin:@db_host:1521/MY_DB_SERVICE.CERN.CH" \
-DoutputDir=dataset:hdfs:/tmp/test5 \
-Dschema=scot  \
-Dpassword=tiger \
-Dparallel=1 \
-DfetchSize=1000 \
-DbatchSize=1000 \
-Dtype_map="COL1:N#ARRAY(N#NUMBER)" \
org.cerndb.hadoop.ingestion.OraDataImporter.OraParquetImport
```


##Example 2
Importing a dynamic view 
```
java -cp $JAR_NAME:$(hadoop classpath) \
-Dsql="select m.*,d.utc_stamp,d.destination from log.meta_fundamental m,og.data_fundamental d where m.FUNDAMENTAL_ID=d.FUNDAMENTAL_ID" \
-DjdbcURI="jdbc:oracle:thin:@db_host:1521/MY_DB_SERVICE.CERN.CH" \
-DoutputDir=dataset:hdfs:/tmp/test5 \
-Dschema=scot  \
-Dpassword=tiger \
-Dparallel=6 \
-DfetchSize=10000 \
-DbatchSize=10000 \
-Dtype_map="FUNDAMENTAL_ID:DECIMAL,FUNDAMENTAL_NAME:STRING,FUNDAMENTAL_TYPE:N#STRING,UTC_STAMP:TIMESTAMP,DESTINATION:N#STRING" \
org.cerndb.hadoop.ingestion.OraDataImporter.OraParquetImport
```

##Example 3
Importing a matrix of numbers

```
java -cp $JAR_NAME:$(hadoop classpath) 
-Dsql="select variable_id,utc_stamp,value from log.$TABLE_NAME partition($PARTITION_NAME)" \
-DjdbcURI="jdbc:oracle:thin:@db_host:1521/MY_DB_SERVICE.CERN.CH" \
-DoutputDir=dataset:hdfs:/tmp/test5 \
-Dschema=scot  \
-Dpassword=tiger \ 
-Dparallel=1 \
-DfetchSize=1000 \
-DbatchSize=1000 \
-Dtype_map="VALUE:ARRAY(ARRAY(NUMERIC)),VARIABLE_ID:DECIMAL" \
org.cerndb.hadoop.ingestion.OraDataImporter.OraParquetImport
```

