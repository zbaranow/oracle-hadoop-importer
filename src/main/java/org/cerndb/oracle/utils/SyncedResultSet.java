// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.



package org.cerndb.oracle.utils;



//JDBC
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;



//Utils
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;


import java.lang.Integer;
import java.lang.Double;





public class SyncedResultSet
{

        private ResultSet rs;
        public int ncols;
        public Map<Integer, HashMap<String,String>> RowSchema = new HashMap<Integer, HashMap<String,String>>();

        public SyncedResultSet(ResultSet r) throws SQLException
        {
           rs = r;
	   getRowSchema(rs);
           getAvroSchema();
        }

        
        

       public int next(byte[][][] cols,int batchSize,boolean getData)  throws SQLException
        {

            int ret = 0;

            synchronized(this)
            {
		for(int i=0;i<batchSize;i++)
		{
	                if (rs.next()&getData)
        	        {
                	        cols[i][0]=rs.getBytes(1);
                        	cols[i][1]=rs.getBytes(2);
                        	cols[i][2]=rs.getBytes(3);
                                ret++;
                	}
		}
            }
            return ret;
        }


	public int nextRow(byte[][][] cols,int batchSize,boolean getData)  throws SQLException
        {

            int ret = 0;

            synchronized(this)
            {
                for(int i=0;i<batchSize;i++)
                {
                        if (rs.next()&getData)
                        {
				for(int j=0; j<ncols;j++)
                                	cols[i][j]=rs.getBytes(j+1);
                                ret++;
                        }
                }
            }
            return ret;
        }

        private void getRowSchema(ResultSet resultSet ) throws SQLException
	{
		ResultSetMetaData meta = resultSet.getMetaData();
                ncols = meta.getColumnCount();
                System.out.println("Number of columns: "+ncols);
		HashMap<String,String> rowDetails;
		for (int i=1; i<=ncols;i++)
                {
		  
                   rowDetails = new HashMap<String,String>();
		   rowDetails.put("name",meta.getColumnLabel(i));
                   rowDetails.put("type", meta.getColumnTypeName(i));
		   try{
                   	Class test = Class.forName(meta.getColumnClassName(i));
		   }
		   catch(ClassNotFoundException e){
			System.out.println(e.getMessage());

				
		   }
//		   rowDetails.put("class", mapClassName(meta.getColumnClassName(i)));
		   rowDetails.put("class", meta.getColumnClassName(i));
                   rowDetails.put("avro",mapType2Avro(meta.getColumnClassName(i),meta.getColumnLabel(i)));
               
                       
		   RowSchema.put(i, rowDetails);
		   System.out.println(i+": "+RowSchema.get(i).get("name")+", "+RowSchema.get(i).get("type")+", "+RowSchema.get(i).get("class")+", "+RowSchema.get(i).get("avro"));

                }

	}
/*	private String mapClassName(String cls)
	{
		if (cls=="oracle.jdbc.OracleArray")
			return "java.sql.Array";

		return cls;
	}*/
        public String getAvroSchema() throws SQLException
	{
	     
	     String namespace="cern.ch";
             String recordName="record";
	     String recordType="record";
            
	     String AvroSchema="{"+
		"  \"namespace\" : \""+namespace+"\","+
		"  \"name\": \""+recordName+"\","+
		"  \"type\" :  \""+recordType+"\","+
		"  \"fields\" :[";

             for (int i=0; i<RowSchema.size();i++)
	     {
		if(i!=0) AvroSchema+=",";

		AvroSchema+="{\"name\": \""+RowSchema.get(i+1).get("name")+"\", \"type\":"+RowSchema.get(i+1).get("avro")+"}";
	     }
             AvroSchema+="]}";
		

//             System.out.println(AvroSchema);
	     return AvroSchema;
		
	}
        private String mapType2Avro(String type,String name)
	{
		String ret=null;

                if(type.equals("oracle.jdbc.OracleArray")) ret= "{\"type\": \"array\", \"items\": \"double\"}";

		if(name.equals("VARIABLE_ID")) {
			ret= "\"long\"";
		}



		if (type=="java.sql.Timestamp"||(type=="java.math.BigDecimal"&&name!="VARIABLE_ID")||type=="oracle.sql.TIMESTAMPTZ"||type=="oracle.sql.TIMESTAMP") ret= "\"double\"";
		if (type=="java.lang.String") ret= "\"string\"";

		if(type.equals("java.sql.Array")) ret= "{\"type\": \"array\", \"items\": \"double\"}";

		  if(name.equals("VARIABLE_ID")) {
                        ret= "\"long\"";
                }



		if (ret==null)
			ret= "\"string\"";
		return "["+ret+",\"null\"]";
	}
	

   }
