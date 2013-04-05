/*
 * Copyright (C) 2013 The MaGDAA Project
 *
 * This file is part of the MaGDAA Rhizome Analysis software
 *
 * MaGDAA Rhizome Analysis software is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * This source code is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this source code; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.magdaaproject.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.configuration.Configuration;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * Various utility methods for accessing the database
 */
public class DatabaseUtils {
	
	/*
	 * public constants
	 */
	public static final String DATABASE_CONST_YES = "Y";
	public static final String DATABASE_CONST_NO  = "N";
	
	/**
	 * get a connection to the MySQL database
	 * 
	 * @param config a Configuration object with database connection details
	 * @return a connection to the database
	 * 
	 * @throws SQLException if something bad happens
	 */
	public static Connection getMysqlConnection(Configuration config) throws SQLException {
		
		// check on the parameters
		if(config == null) {
			throw new IllegalArgumentException("the config parameter is required");
		}
		
		// get a connection to the data
		MysqlDataSource datasource = new MysqlDataSource();
		
		datasource.setServerName(config.getString("db.host"));
		datasource.setDatabaseName(config.getString("db.database"));
		datasource.setUser(config.getString("db.user"));
		datasource.setPassword(config.getString("db.password"));
		
		return datasource.getConnection();
	}
	
	/**
	 * check to see if a table already exists in the database
	 * 
	 * @param connection a valid connection to the database
	 * @param tableName the name of the table to check
	 * @return true if the table exists, false if it does not
	 * @throws SQLException if something bad happens
	 */
	public static boolean doesTableExist(Connection connection, String tableName) throws SQLException { 
		
		boolean tableExists = false;
		
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet tablesList = metaData.getTables(null, null, tableName, null);
		
		if(tablesList.next()) {
			tableExists = true;
		}
		
		tablesList.close();
		tablesList = null;
		
		metaData = null;
		
		return tableExists;
	}

}
