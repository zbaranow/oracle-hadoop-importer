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
	PreparedStatement pstmt=null;
	
	public void OraConnect(String URI,String schema,String password)
	{
		try {

                        Class.forName("oracle.jdbc.driver.OracleDriver");

                } catch (ClassNotFoundException e) {

                        System.out.println("Where is your Oracle JDBC Driver not found");
                        e.printStackTrace();
                        System.exit(1);

                }
		try {

                        connection = DriverManager.getConnection(URI,schema,password);

                } catch (SQLException e) {

                        System.out.println("Connection Failed!");
                        e.printStackTrace();
                        System.exit(1);

                }

                if (connection != null) {
                } else {
                        System.out.println("Failed to make connection!");
                        System.exit(1);
                }

		
		

	}
	public ResultSet execute(String sql) throws SQLException
	{
	     pstmt = connection.prepareStatement(sql);
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
