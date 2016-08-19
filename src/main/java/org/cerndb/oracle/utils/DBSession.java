// Copyright (C) 2016, CERN
// This software is distributed under the terms of the GNU General Public
// Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as Intergovernmental Organization
// or submit itself to any jurisdiction.




package org.cerndb.oracle.utils;


//JDBC
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
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
import java.util.Date;





public class DBSession
{
	Connection connection=null;
	Connection diag=null;
	PreparedStatement pstmt=null;
//	private int sid,instance;
//	private string sch,pass,uri;
	public int fetchSize=1000;

	
	public static Connection OraConnect(String URI,String schema,String password)
	{
		Connection c=null;
		try {

                        Class.forName("oracle.jdbc.driver.OracleDriver");

                } catch (ClassNotFoundException e) {

                        System.out.println("Where is your Oracle JDBC Driver not found");
                        e.printStackTrace();
                        System.exit(1);

                }
		try {

                        c = DriverManager.getConnection(URI,schema,password);

                } catch (SQLException e) {

                        System.out.println("Connection Failed!");
                        e.printStackTrace();
                        System.exit(1);

                }

                if (c != null) {
                } else {
                        System.out.println("Failed to make connection!");
                        System.exit(1);
                }
		return c;
		
		

	}
	public void Connect(String URI,String schema,String password)
	{
//		sch=schema;
//		pass=password;
//		uri=URI;
		connection=OraConnect(URI,schema,password);
//		getConnectionInfo();
	}
	public ResultSet execute(String sql) throws SQLException
	{
	     pstmt = connection.prepareStatement(sql);
	     pstmt.setFetchSize(fetchSize);
             if(pstmt.execute())
  	        return pstmt.getResultSet();
             return null;
	}	
	public void setDirectReads(boolean direct) throws SQLException
	{
		if (direct)
			execute("alter session set \"_serial_direct_read\"=always");
		else 
			execute("alter session set \"_serial_direct_read\"=false");

	}
/*
	private void getConnectionInfo()
	{
		try{
			ResultSet rs = execute("select sys_context('USERENV','INSTANCE') inst_id,sys_context('USERENV','SID') sid from dual");
			rs.next();
			instance=rs.getInt(1);
			sid=rs.getInt(2);
	
		}
		catch(SQLException se)
		{
			System.out.println("Failed to get connection info");
		}
		
		
	}
	public void getConnectionStats(Map<String,int> stats)
	{
		String sql = "select STATISTIC#,value from v$sesstat where sid=:1 and statistic# in (";
		int i=0;
		for (String stat : stats.keySet()) {
		    if (i!=0)
			sql+=",";
 		    
		}
	}
*/
	public void close()
	{
	        try{

			pstmt.close();
		}
		catch(SQLException e)
		{}
		finally
		{
			try{
				connection.close();		
			}
			catch(SQLException e)
			{}
		}
	}
	
   
}
