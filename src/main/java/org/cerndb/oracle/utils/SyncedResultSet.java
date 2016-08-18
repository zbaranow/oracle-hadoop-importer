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

        public SyncedResultSet(ResultSet r) throws SQLException
        {
           rs = r;
	   ncols= rs.getMetaData().getColumnCount();
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

   }
