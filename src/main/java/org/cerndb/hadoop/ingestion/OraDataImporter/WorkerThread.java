// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.



package org.cerndb.hadoop.ingestion.OraDataImporter;


//Home made tools
import org.cerndb.oracle.utils.SyncedResultSet;
import org.cerndb.utils.Schema;
import org.cerndb.utils.SchemaFactory;




//Exceptions
import java.io.IOException;
import java.sql.SQLException;



//Utils
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;





public class WorkerThread implements Runnable {
	
	private Thread t;
	private String threadName;
	private SyncedResultSet sds;
	private Schema schema;
        private String DatasetURI=OraParquetImport.OUTPUT_PATH;

	private boolean writeData=true;
	private boolean readData=true;

	private static long rowsInserted=0;
        private static int threads_finished_ok=0;
	private static int threads_started=0; 
	private static boolean terminateAll=false;


        private int batchSize=OraParquetImport.THREAD_BATCH_SIZE;


      public WorkerThread(String name,SyncedResultSet s,Schema sch) throws IOException{
        threadName = name;
        sds=s;
	schema=sch;
        //System.out.println("Creating " +  threadName );
      }
      public static int getSuccessfulThreads()
      {
	return threads_finished_ok;
      }

      public static void terminateAll()
      {
	    terminateAll=true;
      }
      private static void updateStats(int rows)
      {
		   rowsInserted+=rows;
      }
      public static long getStats()
      {
	return rowsInserted;
      }	
      public void join() throws InterruptedException
      {
        t.join();
      }
	public Thread.State getState()
	{
		return t.getState();
	}
      
	public void run() {
         System.out.println("Running " +  threadName );
	    
	    DataWriter dw;
            try{

		     dw = new DataWriter(schema);
                     dw.InitDataset(DatasetURI, SchemaFactory.getAvroSchema(schema,OraParquetImport.AVRO_CLASS_MAP,OraParquetImport.AVRO_TYPE_MAP,OraParquetImport.AVRO_NAME_MAP));
		     dw.openWriter();          
	    }
	    catch(IOException ioe)
	    {
		System.out.println(ioe.getMessage());
		return;
	    }
            /*catch (SQLException sqle)
	    {
		System.out.println(sqle.getMessage());
		return;
	    }*/
            try{
 
            
                 byte[][][] rows = new byte[batchSize][schema.root.size()][];
                 int rows_num=batchSize;

                 while(rows_num == batchSize&&!terminateAll ){ 
			rows_num = sds.nextRow(rows, batchSize,readData);
			for(int i = 0; i < rows_num; i++){
				byte[][] rowcols = new byte[schema.root.size()][];
				for(int j=0; j <schema.root.size(); j++){
					
                                        rowcols[j] =  rows[i][j];
                                        
				}

                               dw.write(rowcols);

			} 
		        updateStats(rows_num);
		    }
		
		  threads_finished_ok++;
		}//try            
		catch (SQLException e) {
                        System.out.println("Failed to execute statement! "+ e.toString());


		}
		finally {
      			if (dw != null) {
		            dw.close();
		        }
		}  
	 	

     }

     

      public void start ()
      {
          System.out.println("Starting " +  threadName );
          if (t == null)
          {
            t = new Thread (this, threadName);
            t.start ();
	    threads_started++;

          }

      }


   }

