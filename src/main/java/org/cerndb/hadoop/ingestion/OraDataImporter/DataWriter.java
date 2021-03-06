// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.


package org.cerndb.hadoop.ingestion.OraDataImporter;


import org.cerndb.oracle.utils.OraDataDecoder;
import org.cerndb.utils.SchemaFactory;
import org.cerndb.utils.SchemaElement;
import org.cerndb.utils.Schema;
import org.cerndb.utils.DataType;





//Exceptions
import java.text.ParseException;
import java.lang.NumberFormatException;
import java.io.UnsupportedEncodingException;
import java.io.IOException;



//Utils
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;



import java.nio.charset.StandardCharsets;




//Kite related

import org.kitesdk.data.Dataset;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.CompressionType;
import static org.apache.avro.generic.GenericData.Record;
import org.kitesdk.data.DatasetOperationException;

   class DataWriter{

	private DatasetDescriptor dsd;
	private  Dataset<Record> ds;
	DatasetWriter<Record> writer;
	GenericRecordBuilder builder;
        private String DataSetURI,Schema;
	private Schema schema;





       public static  Dataset<Record> CreateDataset(String path,DatasetDescriptor dd) throws IOException{

		Dataset<Record> dataset = Datasets.create(
	        path, dd, Record.class);
		System.out.println("====>Dataset *** "+path+" *** created with schema:\n"+dd.getSchema().toString());		
		return dataset;
		
       }
       //check if exists, if not creates a new one
       public void InitDataset(String URI,Schema sch) throws IOException
       {
		//Initializing fields
		 DataSetURI=URI;
		 schema=sch;

		dsd = new DatasetDescriptor.Builder()
                .schemaLiteral(SchemaFactory.getAvroSchema(schema))
                .format(Formats.PARQUET)
                .compressionType(CompressionType.Snappy)
                .build();

		if(Datasets.exists(DataSetURI))
		{
			ds=Datasets.load(DataSetURI,Record.class);
			if(!ds.getDescriptor().getSchema().toString().equals(dsd.getSchema().toString()))
			{
				
				throw new IOException("Dataset "+DataSetURI+" exists but has different schema\n"+
				"Loaded schema:\n"+ds.getDescriptor().getSchema().toString()+
				"\nDesired schema:\n"+dsd.getSchema().toString());
			}
		}
		else 
			ds=CreateDataset(DataSetURI,dsd);
       }

       public DatasetWriter<Record> openWriter(){
		builder = new GenericRecordBuilder(dsd.getSchema());
		writer=ds.newWriter();
                return writer;
	}
       public void write(Record r){
		writer.write(r);
	}


	public void write(byte[][] cols)
	{
		for (int i=0; i<schema.root.size(); i++)
		{
			try{
                              builder.set(schema.root.get(i).elementName,OraDataDecoder.castColType(cols[i],schema.root.get(i)));
				
			}
			catch(ArrayIndexOutOfBoundsException e)
			{
//				System.out.println(new String(cols[0],StandardCharsets.UTF_8));
				dumpRow(cols);
				throw e;
			}
			catch (NullPointerException npe)
			{
//				System.out.println(new String(cols[i],StandardCharsets.UTF_8));
				dumpRow(cols);

				throw npe;
			}
		}
		try
		{
			writer.write(builder.build());
		}
		catch(DatasetOperationException e)
		{
			dumpRow(cols);
			throw e;
		}
		
	}
	private void dumpRow(byte[][] data)
	{
		System.out.println("DATA DUMP:");
		for (int i=0; i<schema.root.size(); i++)
		{
		 	System.out.println("COL"+i+":");
			dumpBytes(data[i]);
			System.out.println("Decoded value:");
			String val="ERROR! <=====================";
			try{
				val = OraDataDecoder.castColType(data[i],schema.root.get(i)).toString();
			}
			catch(ArrayIndexOutOfBoundsException e)
			{}
			catch(NullPointerException npe)
			{}
			System.out.println(val);
			
		}
	}
	public static void dumpBytes(byte[] data)
         {
                 for(int i = 0; i < data.length ; i ++ ){
                        System.out.println(i+"/"+data.length+": "+getInt(data[i]));
                }

         }
	public static int getInt(byte b)
        {
                return b & 0xFF;

        }

	


        public void close(){
		writer.close();
	}
   }

