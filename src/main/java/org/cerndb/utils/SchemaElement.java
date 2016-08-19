// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.

package org.cerndb.utils;

import java.util.Map;
import java.util.HashMap;

public	class SchemaElement{
	public SchemaElement parent=null;
	public SchemaElement child=null;
//	public Map<Integer,SchemaElement> children;
	public String elementName;
	public String elementType;
	public String elementNativeType;
	public DataType elementInternalType;
	public boolean nullable=false;
	public int elementPosition;
	
	public String toString()
	{
		return elementPosition+": Name: "+elementName+" Type: "+elementType+" Native: "+elementNativeType+" Internal: "+DataType.toString(elementInternalType)+" Nullable: "+String.valueOf(nullable);
	}
}




