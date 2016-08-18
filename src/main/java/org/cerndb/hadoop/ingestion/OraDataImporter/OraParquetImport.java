// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.




package org.cerndb.hadoop.ingestion.OraDataImporter;

//Home made stuff
import org.cerndb.oracle.utils.OraDataDecoder;
import org.cerndb.oracle.utils.DBSession;
import org.cerndb.oracle.utils.SyncedResultSet;
import org.cerndb.utils.Statistics;
import org.cerndb.utils.StatType;
import org.cerndb.utils.SchemaFactory;
import org.cerndb.utils.Schema;


//JDBC
import java.sql.ResultSet;


import java.text.SimpleDateFormat;

//Exceptions
//import java.text.ParseException;
//import java.lang.NumberFormatException;
//import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.sql.SQLException;



//Utils
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;


import java.lang.Integer;
import java.lang.Double;
import java.util.Date;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.math.BigInteger;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class OraParquetImport{

	private static final int THREADS = Integer.parseInt(System.getProperty("parallel", "1"));
	private static final String SQL = System.getProperty("sql", "Null");
	public static final String OUTPUT_PATH = System.getProperty("outputDir", "Null");
	private static final String DB_URI = System.getProperty("jdbcURI", "Null");
	private static final String DB_SCHEMA = System.getProperty("schema", "Null");
	private static final String DB_PASS = System.getProperty("password", "Null");
	private static final int DB_FETCH_SIZE = Integer.parseInt(System.getProperty("fetch_size", "10000"));
        public static final int THREAD_BATCH_SIZE = Integer.parseInt(System.getProperty("batch_size", "10000"));
	public static final String AVRO_CLASS_MAP = System.getProperty("avro_class_map", "").replace('\'','"');
        public static final String AVRO_TYPE_MAP = System.getProperty("avro_type_map", "").replace('\'','"');
        public static final String AVRO_NAME_MAP = System.getProperty("avro_name_map", "").replace('\'','"');
        public static final String NAME_2_TYPE_MAP = System.getProperty("type_map", "");






        //checks


        public static void main(String[] argv) {

                OraParquetImport imp = new OraParquetImport();

		System.out.println(AVRO_TYPE_MAP);
		
		//parameter checks
		if(SQL.equals("Null")||OUTPUT_PATH.equals("Null")||DB_URI.equals("Null")||DB_SCHEMA.equals("Null")||DB_PASS.equals("Null"))
		{
			System.out.println("Not enough parameters provided");
			System.out.println("THREADS: " +THREADS);
                        System.out.println("SQL: " +SQL);
                        System.out.println("DBURI: " +DB_URI);
                        System.out.println("DB_SCHEMA: " +DB_SCHEMA);
			if (OUTPUT_PATH.equals("Null"))
	                        System.out.println("DB_PASS: " +DB_PASS);
			else
				System.out.println("DB_PASS: ********");

			printHelp();
                        System.exit(1);

		}
                try{
                	imp.run(argv);
                }
		 catch(IOException ioe)
            {System.out.println(ioe.getMessage());}

		


        }
	public static void printHelp()
	{
		String text="\n"+
			"Usage: Command -Dsql=<text> -DjdbcURI=<text> -DoutputDir=<text> [-D..options..]";
		System.out.println(text);
	}
        public void run(String[] argv) throws IOException, NumberFormatException
        {




		//1. Opening connection to db
		DBSession connection = new DBSession();
		connection.OraConnect(DB_URI,DB_SCHEMA,DB_PASS);
                
		ResultSet rs = null;

		//initializing statistics
                Statistics stats = new Statistics();

		try{

		   connection.setDirectReads(true);

		   rs = connection.execute(SQL);

		   //2. Create synchronaized result set
	           SyncedResultSet sds = new SyncedResultSet(rs);	
		
		   
		   //3. Infer result set schema and initialize target dataset
		
		   Schema schema = SchemaFactory.inferSchema(rs,NAME_2_TYPE_MAP);
                   System.out.println("Schema:");
                   System.out.println(SchemaFactory.getAvroSchema(schema));
                   
		   //initializing dataset and verifying schema
		   DataWriter dw = new DataWriter();
                   dw.InitDataset(OUTPUT_PATH, schema);
                   dw.openWriter();
		   dw.close();


                   //4. Start threads

                   System.out.println("Starting threads");

		   List<WorkerThread> threadList = new ArrayList<WorkerThread>();

                   for (int i=0;i<THREADS;i++)
                   {
                        WorkerThread t = new WorkerThread("Thread-"+i,sds,schema);
			threadList.add(t);
                        t.start();
                        if (i==0&&THREADS>1)
                        {
				//wait to initialize dataset by the first thread
/*			     try{

				Thread.sleep(5000);
			     }
                              catch (InterruptedException e){}
*/
				
                        }


                   }
                   stats.updateStat("RowsLoaded",WorkerThread.getStats(),StatType.CUMULATIVE);
	   
		   boolean terminated=false;
		   //watch loop; checking if all threads finished; collecting and printing runtime stats   
		   while(!terminated)
		   {
			try{
				Thread.sleep(10000);
			}
			catch (InterruptedException e){}

			stats.updateStat("RowsLoaded",WorkerThread.getStats(),StatType.CUMULATIVE);
			System.out.println(stats.getFormattedStat("RowsLoaded"));
			terminated=true;
			for (WorkerThread th : threadList)
			{
				if(th.getState()!=Thread.State.TERMINATED)
				{
					terminated=false;
					break;
				}

			}//for
		   }//while
                }//try
                catch (SQLException e) {
                        System.out.println("Failed to execute statement! "+ e.toString());
                        System.exit(1);
                }
                //catch(ParseException e)
//              {}
                finally {
                     connection.close();
//                     System.out.println("Job finished");
                }

		 try{
                                Thread.sleep(5000);
                }
                catch (InterruptedException e){}
                System.out.println(stats.getFormattedStat("RowsLoaded"));

		//Job done - final checks

		if (WorkerThread.getSuccessfulThreads()==THREADS)
		{
			System.out.println("JOB FINISHED SUCCESSFULLY!");

		}
		else
		{
                        System.out.println("JOB FAILED!");

		}
        }

}
