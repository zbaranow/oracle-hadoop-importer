// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.

package org.cerndb.utils;

public enum DataType
{
	INT,LONG,FLOAT,DOUBLE,STRING,TIMESTAMP,TIMESTAMP_LONG,ARRAY;

	public static DataType parse(String type)
	{
		DataType ret=null;
		switch(type.toUpperCase())
		{
			case "NUMBER":
				ret = DataType.DOUBLE;
				break;

			case "CHAR":
				ret = DataType.STRING;
				break;

			case "NUMERIC":
				ret = DataType.DOUBLE;
				break;

			case "FLOAT":
				ret = DataType.FLOAT;
				break;

			case "INT":
				ret = DataType.INT;
				break;

			case "LONG":
				ret = DataType.LONG;
				break;

			case "DECIMAL":
				ret = DataType.LONG;
				break;

			case "STRING":
				ret = DataType.STRING;
				break;

			case "TIMESTAMP":
				ret = DataType.TIMESTAMP;
				break;

			case "TIMESTAMP_LONG":
				ret = DataType.TIMESTAMP_LONG;
				break;
			default:
				ret = null;
				

		}
		return ret;
	}
	public static String toString(DataType type)
	{
		String ret=null;
		switch(type)
		{
			

			case STRING:
				ret = "STRING";
				break;
			case FLOAT:
                                ret = "FLOAT";
                                break;

                        case DOUBLE:
                                ret = "DOUBLE";
                                break;

			case INT:
				ret = "INT";
				break;

			case LONG:
				ret = "LONG";
				break;

			case TIMESTAMP:
				ret = "TIMESTAMP";
				break;

			case TIMESTAMP_LONG:
                                ret = "TIMESTAMP_LONG";
                                break;

			case ARRAY:
				ret = "ARRAY(...)";
				break;
			
		}
		return ret;
	}

}
