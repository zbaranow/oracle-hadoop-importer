// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction. 



package org.cerndb.oracle.utils;


import org.cerndb.utils.DataType;
import org.cerndb.utils.SchemaElement;
//import org.cerndb.hadoop.ingestion.OraDataImporter.WorkerThread;

//JDBC
import oracle.sql.*;

//Exceptions
import java.text.ParseException;
import java.lang.NumberFormatException;
import java.io.UnsupportedEncodingException;

//Utils
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import java.lang.Integer;
import java.lang.Double;
import java.util.Date;
import java.nio.charset.StandardCharsets;
import java.math.BigInteger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;




public class OraDataDecoder
   {
	public static long ARRAY_NULLS_DISCARDED=0;


	 public static String null_action=System.getProperty("action_when_null", "-9990999");

	 public static Object castColType(byte[] data,SchemaElement el)
        {
		if (data==null||data.length==0) return null;//null value

                Object odata=null;
                switch(el.elementInternalType)
                {
                        case STRING:
                                odata = OraSimpleTypeDecoder.castVarchar(data);
                                break;

                        case INT:
                                odata= OraSimpleTypeDecoder.castNumber(data);
                                break;

			case LONG:
                                odata= OraSimpleTypeDecoder.castNumber(data);
                                break;

			case FLOAT:
				odata= OraSimpleTypeDecoder.castNumber(data);
                                break;
	
			case DOUBLE:
				odata= OraSimpleTypeDecoder.castNumber(data);
				break;

                        case TIMESTAMP:
                                odata = OraSimpleTypeDecoder.castTimestamp(data);
                                break;

			case TIMESTAMP_LONG:
				odata = OraSimpleTypeDecoder.castTimestamp(data);
                                break;

			case ARRAY:
				odata = OraArrayDecoder.castArray(data,el);
			break;


                }
                return odata;

        }


   }

 class OraSimpleTypeDecoder
   {

	 public static void dumpBytes(byte[] data)
	 {
		 for(int i = 0; i < data.length ; i ++ ){
                        System.out.println(i+"/"+data.length+": "+getInt(data[i]));
                }

	 }
	 public static Object castTimestamp(byte[] data){
                String date="";
                double nano;
                long unixtime=0;
                int value=0;
                byte[] nanoS= new byte[4];
                for (int i=1;i<data.length;i++){
                        value=getInt(data[i]);
                        switch(i){
                                case 1: date+= String.format("%02d", value - 100 );
                                        break;
                                case 2: date+= String.format("%02d",value);
                                        break;
                                case 3: date+= String.format("%02d",value);
                                        break;
                                case 4: date+= String.format("%02d",value - 1);
                                        break;
                                case 5: date+= String.format("%02d",value - 1);
                                        break;
                                case 6: date+= String.format("%02d",value - 1);
                                        break;
                                default:
                                        nanoS[i-7]=data[i];
                                        break;
                        }
                }
                nano=(new BigInteger(nanoS).intValue())
                        /(double)1000000;

                DateFormat dfm = new SimpleDateFormat("yyMMddHHmmss");
                dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
                try
                {
                        //timestamp in ms
                        unixtime = dfm.parse(date).getTime();


                }
                catch (ParseException e)
                {
                        e.printStackTrace();
                }
                return (Object) (unixtime+nano);

	}

	public static Object castVarchar(byte[] data){
                return (Object)new String(data,StandardCharsets.UTF_8);
        }

	public static Object castNumber(byte[] data)
	{
		NUMBER n=null;
		Double  val= null;

		//check if number is nulled
		if ( data==null || 
		    data.length==0 ||
		    ( data.length==2 & getInt(data[0])==193 && getInt(data[1])==1 )
		) return val;

		try{
			n = new NUMBER(data);
			val=n.doubleValue();
			
		}
		catch(NullPointerException ne)
		{
			dumpBytes(data)	;
			throw ne;
		}
		return val;
	}
		
        public static Object castNumber2(byte[] data){

                boolean positive = true;
                int exp=0;
                int value=0;
                String number=null;


                for (int i=0;i<data.length;i++)
                {
                        value=getInt(data[i]);
                        if(i==0)
                        {
                           if (value==128) return 0;
                           if (value>127)
                           {
                                exp=value-192;
                                number="";

                           }
                           else
                           {
                                positive=false;
                                exp=63-value;
                                number="-";

                           }
                           if (exp<0)
                           {
                                number=String.format("%."+(-exp*2)+"f",0.0);
                           }
                           continue;
                        }
                        if(exp==i-1) number+=".";
                        if(positive)
                        {
                                number+=String.format("%02d",value-1);
                        }
                        else{
                                if(i!=data.length-1)
                                        number+=String.format("%02d",101-value);

                        }


                }
                if(number!=null&&exp>(data.length-1))
                     for(int j=data.length-1;j<exp;j++) number+="00";
                return (Object) Double.valueOf(number);
        }
        public static int getInt(byte b)
        {
                return b & 0xFF;

        }


}
   

//Decodes bytes to arrays of types
//TBD: automatic detection of types (probably 2 integer in array header)
 class OraArrayDecoder
   {
	public static void dumpRawArray(byte[] data)
       	{
               	for(int i = 0; i < data.length ; i ++ ){
                       	System.out.println(i+"/"+data.length+": "+getInt(data[i]));
		}              	
        }
	public static List<Object> castArray(byte[] data,SchemaElement el)
        {
               return getList(data,el);
        }


	public static List<Object> getList(byte[] data,SchemaElement el)
	{
		int state = 1;
		int v=0;
		List<Object> elements;
		int arraySize=0;
		int length=0;
		int elementLength=0;

		Object element=null;
		elements = new ArrayList<Object>();
		for(int i=2;i<data.length;i++)
		{
			v=getInt(data[i]);
			switch(state){
				case 1: //getting length
					if(v==254)
					{
					    arraySize=(new BigInteger(Arrays.copyOfRange(data,i+1,i+5))).intValue();
					    i+=4;
					}
					else
					    arraySize=v;
			//		state=1;
			//                break;
			//	case 1: //length initialized
					if(arraySize==data.length) state=2;
					else {
	                                         dumpRawArray(data);
						 v=data[-1];  //something went wrong,raise an exception
					}
					break;
				case 2:
					//some bytes to be skipped - TO BE CHECK WHAT IS THE MEANING
					i+=v+1;
					if(getInt(data[i])==0)  
						state=3;
					else
					{
						System.out.print(i+":"+getInt(data[i]));
						v=data[-1];//raise exception
					}
					break;
				case 3: //geting elements number						
					if(v==254)
                                        {
                                             length=(new BigInteger(Arrays.copyOfRange(data,i+1,i+5))).intValue();
                                             i+=4;
                                        }
                                        else
                                           length=v;

                                        state=4;

                                        break;

				case 4: //getting elements value
					if(v==255)
					{
					    elementLength=0;
					}
					else
						if(v==254)
	                                        {
	                                            elementLength=(new BigInteger(Arrays.copyOfRange(data,i+1,i+5))).intValue();
	                                            i+=4;
	                                        }
	                                        else
	                                            elementLength=v;
						
						element=OraDataDecoder.castColType(Arrays.copyOfRange(data, i+1, i+elementLength+1),el.child);
						if (element!=null)
						{
							elements.add(element);		
						}
						else 
						if(OraDataDecoder.null_action.equals("DISCARD")) //IGNORING NULL ==> Kite does not support null values in arrays
						{
							length--;
							OraDataDecoder.ARRAY_NULLS_DISCARDED++;
							System.out.println("NULL element in array discareded!!!!!!!<===============================");
						}
						else if(OraDataDecoder.null_action.equals("DISCARD_ROW"))
						{
							
						}
						else
						{
							OraDataDecoder.ARRAY_NULLS_DISCARDED++;
							elements.add(Double.parseDouble(OraDataDecoder.null_action));
						}
					

					i+=elementLength;

					break;
			}
		}
		//check
		
		if (elements.size()!=length) {
			System.out.println(elements.size()+"!="+length);
			 dumpRawArray(data);
			 v=data[-1];
		}
	
	  return elements;		
      }
	 private static int getInt(byte b)
        {
                return b & 0xFF;
	}

	
   }   	    
   


