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

	
		
	private void addElement(SchemaElement e)
	{

		if(root==null)
			root = new HashMap<Integer, SchemaElement>();
		root.put(root.size(),e);		
	}
	public void addElement(int position,String name,String type,String nativeType)
	{
		SchemaElement el = new SchemaElement();
		el.elementName=name;
		el.elementType=type;
		el.elementNativeType=nativeType;
		el.elementPosition=position;
		addElement(el);
	}

}
