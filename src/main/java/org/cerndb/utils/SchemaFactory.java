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


/*
	private static void  parseCustomAvroMapping (String customMapping,Map<String,String> mapping)
	{
		if (customMapping==null||customMapping=="") return;
		for(String map: customMapping.split("&"))
		{
                  //     System.out.println("V:"+map);

			String[] parts = map.split("=");
			mapping.put(parts[0],parts[1]);
	
		}		

	}
	
	private static Map<String,String> initAvroClassMapping(String customMapping)
	{
		
		Map<String,String> mapping = new HashMap<String,String>();
		mapping.put("java.Math.BigDecimal","\"long\"");
                mapping.put("oracle.jdbc.OracleArray","{\"type\": \"array\", \"items\": \"double\"}");
                mapping.put("java.lang.String","\"string\"");
		
		

		if (customMapping!=null&&customMapping!="")
		{
			parseCustomAvroMapping(customMapping,mapping);
		}
		return mapping;
		
	}
	 private static Map<String,String> initAvroTypeMapping(String customMapping)
        {
		

                Map<String,String> mapping = new HashMap<String,String>();
		mapping.put("NUMBER","\"long\"");
                mapping.put("CHAR","\"string\"");
                mapping.put("TIMESTAMP","\"double\"");


                if (customMapping!=null&&customMapping!="")
                {
		//	System.out.println("M:"+customMapping);
                        parseCustomAvroMapping(customMapping,mapping);
			
                }
                return mapping;

        }
        public static String getAvroType (SchemaElement e,String customClassMapping,String customTypeMapping,String customNameMapping)
	{
		Map<String,String> classMapping=initAvroClassMapping(customClassMapping);
                Map<String,String> typeMapping=initAvroTypeMapping(customTypeMapping);
                Map<String,String> nameMapping= new HashMap<String,String>();
                parseCustomAvroMapping(customNameMapping,nameMapping);


                String eType ="";
                if (classMapping.containsKey(e.elementNativeType))
                        eType=classMapping.get(e.elementNativeType);
                if (typeMapping.containsKey(e.elementType))
                        eType=typeMapping.get(e.elementType);
                if (nameMapping.containsKey(e.elementName))
                        eType=nameMapping.get(e.elementName);



		return eType;
	}
	public static String getAvroSchema(Schema schema,String customClassMapping,String customTypeMapping,String customNameMapping)
	{
		String namespace="cern.ch";
		String recordName="record";
		String recordType="record";
			
		
		//initialising mappings
		Map<String,String> classMapping=initAvroClassMapping(customClassMapping);
		Map<String,String> typeMapping=initAvroTypeMapping(customTypeMapping);
		Map<String,String> nameMapping= new HashMap<String,String>();
		parseCustomAvroMapping(customNameMapping,nameMapping);

		String AvroSchema="{"+
		"  \"namespace\" : \""+namespace+"\","+
		"  \"name\": \""+recordName+"\","+
		"  \"type\" :  \""+recordType+"\","+
		"  \"fields\" :[";
		for (int i=0;i<schema.root.size();i++)
		{
			if(i!=0) AvroSchema+=",";
			SchemaElement e = schema.root.get(i);
		//	System.out.println(e.elementNativeType+" "+classMapping.get(e.elementNativeType));
			String eType ="";
			if (classMapping.containsKey(e.elementNativeType))
				eType=classMapping.get(e.elementNativeType);
			if (typeMapping.containsKey(e.elementType))
				eType=typeMapping.get(e.elementType);
			if (nameMapping.containsKey(e.elementName))
				eType=nameMapping.get(e.elementName);


			AvroSchema+="{\"name\": \""+e.elementName+"\", \"type\":"+getAvroType(e,customClassMapping,customTypeMapping,customNameMapping)+"}";
		}
		
		AvroSchema+="]}";

		return AvroSchema;
	}
*/
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
		
		
		switch(e.elementInternalType)
		{
			case NUMERIC:
                                avro += "\"double\"";
                                break;
                        case STRING:
                                avro += "\"string\"";
                                break;
                        case DECIMAL:
                                avro +=  "\"long\"";
                                break;
                        case TIMESTAMP:
                                avro += "\"double\"";
                                break;
                        case NUMERICARRAY:
                                avro += "{\"type\":\"array\",\"items\":\"double\"}";
                                break;
                        case STRINGARRAY:
                                avro += "{\"type\":\"array\",\"items\":\"string\"}";
                                break;
                        case NUMERICMATRIX:
                                avro += "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"double\"}}";
                                break;
                        case STRINGMATRIX:
				avro += "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"string\"}}";
                                break;
			case ARRAY:
				avro +="{\"type\":\"array\",\"items\":"+element2avro(e.child,true)+"}";
				break;

		}
		if(!nested) avro+="}";
		return avro;	
		
		
		
		
	}



}
