// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.

package org.cerndb.utils;

import java.util.Map;
import java.util.HashMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;

public class SchemaFactory
{

	public static Schema inferSchema(ResultSet resultSet ,String Smap) throws SQLException
	{
		Schema schema = new Schema();
		schema.parseMapping(Smap);

		schema.root = new HashMap<Integer, SchemaElement>();


		

		ResultSetMetaData meta = resultSet.getMetaData();
                int ncols = meta.getColumnCount();
                System.out.println("Number of columns: "+ncols);
		HashMap<String,String> rowDetails;
		for (int i=0; i<ncols;i++)
                {
		  
		   SchemaElement col = new SchemaElement();
		   schema.addElement(i,meta.getColumnLabel(i+1),meta.getColumnTypeName(i+1),meta.getColumnClassName(i+1));
		   
		   System.out.println(schema.root.get(i).toString());
				
		 }
		return schema;

	}
	public static Schema newSchema()
	{
		return new Schema();
	}


	public static String getAvroSchema(Schema schema)
	{
		String namespace="cern.ch";
                String recordName="record";
                String recordType="record";
		 String AvroSchema="{\n"+
                "  \"namespace\" : \""+namespace+"\",\n"+
                "  \"name\": \""+recordName+"\",\n"+
                "  \"type\" :  \""+recordType+"\",\n"+
                "  \"fields\" :\n[";
		for (int i=0;i<schema.root.size();i++)
                {
			if(i!=0) AvroSchema+=",\n";
			AvroSchema+="\t"+element2avro(schema.root.get(i),false);
			
		}
		AvroSchema+="\n]\n}";

                return AvroSchema;

	}
	public static String element2avro(SchemaElement e,boolean nested)
	{
		String avro="";
		if(!nested)
			avro+="{\"name\":\""+e.elementName+"\","+
			"\"type\":";
		
		if(e.nullable)
		{
			avro+="[";
		}	
		switch(e.elementInternalType)
		{
			case DOUBLE:
                                avro += "\"double\"";
                                break;
			case FLOAT:
				avro += "\"float\"";
				break;
                        case STRING:
                                avro += "\"string\"";
                                break;
			case INT:
				avro += "\"int\"";
				break;
                        case LONG:
                                avro +=  "\"long\"";
                                break;
                        case TIMESTAMP:
                                avro += "\"double\"";
                                break;
			case TIMESTAMP_LONG:
				avro += "\"long\"";
				break;
			case ARRAY:
				avro +="{\"type\":\"array\",\"items\":"+element2avro(e.child,true)+"}";
				break;

		}
		if(e.nullable)
		{
		    avro+=",\"null\"]";
		}
		if(!nested) avro+="}";
		return avro;	
		
		
		
		
	}



}
