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
		el.elementInternalType=getInternalType(el);
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
	public void parseMapping(String mapping)
	{
		if (mapping==null||mapping=="") return;
                for(String map: mapping.split(","))
                {
                        String[] parts = map.split(":");
                        addMapping(parts[0],DataType.parse(parts[1]));

                }
 
	}

}
