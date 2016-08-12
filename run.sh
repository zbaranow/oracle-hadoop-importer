/usr/java/jdk1.7.0_67-cloudera/bin/java -cp ./target/kite-java-importer-1.0-SNAPSHOT.jar:/var/lib/sqoop/ojdbc.jar:$(hadoop classpath) org.kite.DBImporter.OraParquetImport $1
