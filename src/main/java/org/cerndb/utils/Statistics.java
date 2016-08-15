// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.



package org.cerndb.utils;

//Utils
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;


import java.lang.Integer;
import java.lang.Double;
import java.util.Date;



   
 public  class Statistics
   {
	Map<String,ArrayList<Statistic>> objectStats = null;
	Map<String,StatType> statsType = null;
	long systemStart;

	public Statistics()
	{
		systemStart = System.currentTimeMillis();
	}
	public Statistics(long startTime)
	{
		systemStart = startTime;
	}
	public void updateStat(String name,Object value,StatType stype)
	{
		if(statsType==null)
			statsType = new HashMap<String,StatType>();

		if(!statsType.containsKey(name))
			statsType.put(name,stype);

		if(objectStats==null)
			objectStats = new HashMap<String,ArrayList<Statistic>>();

		if(!objectStats.containsKey(name))
		{
			objectStats.put(name,new ArrayList<Statistic>());
		}
		objectStats.get(name).add(new Statistic(value,System.currentTimeMillis()));
		
	}
	public String getFormattedStat(String name)
	{
		//should check the type of stat
		//assuming cumlative
		String ret="";
		if (objectStats.get(name).size()<2) return null;
		switch(statsType.get(name))
		{
			case CUMULATIVE:
				Statistic prev=objectStats.get(name).get(objectStats.get(name).size()-2);				
                                Statistic current=objectStats.get(name).get(objectStats.get(name).size()-1);
				ret="Total "+name+": "+current.value+" in "+(System.currentTimeMillis()-systemStart)+"ms, ";
				ret+= "Avg rate:"+String.format("%.02f",((long)current.value)/((float)(current.updateTime-systemStart)/1000))+" rows/s, ";

				ret+="Last "+((current.updateTime-prev.updateTime)/1000)+"s rates:";
				ret+=String.format("%.02f",((long)current.value-(long)prev.value)/((float)(current.updateTime-prev.updateTime)/1000))+" rows/s";
					

				break;
		}
		return ret;
	}
	class Statistic
	{
		public long updateTime;
		public Object value;
		Statistic(Object svalue, long time)
		{
			value=svalue;
			updateTime=time;
		}
	}
        
   }   
