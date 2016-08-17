// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.

package org.cerndb.utils;

import java.util.Map;
import java.util.HashMap;


public class Schema
{
	public Map<Integer,SchemaElement > root = null;

        public Map<String,DataType> name2type = null;
	
		
	private void addElement(SchemaElement e)
	{

		if(root==null)
			root = new HashMap<Integer, SchemaElement>();
		e.elementInternalType=getInternalType(e);
		root.put(root.size(),e);		
	}
	public void addElement(int position,String name,String type,String nativeType)
	{
		SchemaElement el = new SchemaElement();
		el.elementName=name;
		el.elementType=type;
		el.elementNativeType=nativeType;
		el.elementPosition=position;
		setInternalType(el);
		addElement(el);
	}
	public void addMapping(String name, DataType type)
	{
		if(name2type==null)
			name2type=new HashMap<String,DataType>();
		name2type.put(name,type);	
	}
	public DataType getInternalType(SchemaElement e)
	{
		
		if(name2type.containsKey(e.elementName))
			return name2type.get(e.elementName);
		return DataType.parse(e.elementType);
	}
	public void setInternalType(SchemaElement e)
	{
		if(name2type.containsKey(e.elementName))
			e.elementInternalType=name2type.get(e.elementName);
		if(e.elementInternalType==DataType.ARRAY)
		{
			int i=1;
			SchemaElement se=e;
			while(name2type.containsKey(e.elementName+i))
			{
				SchemaElement sne = new SchemaElement();
				sne.elementName=e.elementName+i;
				sne.elementInternalType=name2type.get(e.elementName+i);
				se.child=sne;
				se=sne;
				i++;
			}
		}
	}
	public void parseMapping(String mapping)
	{
		if (mapping==null||mapping=="") return;
                for(String map: mapping.trim().split(","))
                {
                        String[] parts = map.split(":");
			if(!parts[1].toUpperCase().contains("ARRAY"))
			{
				DataType p = DataType.parse(parts[1]);
			
                        	addMapping(parts[0],DataType.parse(parts[1].trim()));
			}
			else  //It is an array
			{
                                addMapping(parts[0],DataType.ARRAY);

				boolean done=false;
				String subpart=parts[1].toUpperCase();
				int i=1;
				while(!done)
				{
					done=true;
					subpart = removeBruckets(subpart);
					if(subpart.contains("ARRAY"))
					{
						done=false;
						addMapping(parts[0]+i,DataType.ARRAY);
					}
					else
	               	                	addMapping(parts[0]+i,DataType.parse(subpart.trim()));
					i++;
				}

				
			}

                }
 
	}
	private String removeBruckets(String input)
	{
		return input.substring(input.indexOf('(')+1,input.lastIndexOf(')'));
	}

}
