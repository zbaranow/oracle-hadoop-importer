JAR_NAME=OraDataImporter-1.0-SNAPSHOT.jar

scp ./target/$JAR_NAME zbaranow@itrac1501:
ssh -K zbaranow@itrac1501 "java -cp ./$JAR_NAME:\$(hadoop classpath) \
-Dsql=\"select variable_id,utc_stamp,value from lhclog.data_vectornumeric partition(PART_DVN_$1)\" \
-DjdbcURI=\"jdbc:oracle:thin:@itrac50005:10121/ACCLOG.cern.ch\" \
-DoutputDir=dataset:hdfs:/tmp/PART_DVN_$1 \
-Dschema=hadoop_data_reader \
-Dpassword=impala1234 \
-Dparallel=10 \
-DfetchSize=100 \
-DbatchSize=100 \
org.cerndb.hadoop.ingestion.OraDataImporter.OraParquetImport"
