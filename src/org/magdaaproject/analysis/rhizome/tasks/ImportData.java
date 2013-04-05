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
package org.magdaaproject.analysis.rhizome.tasks;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.configuration.Configuration;
import org.magdaaproject.utils.DatabaseUtils;
import org.magdaaproject.utils.StringUtils;

/**
 * class to undertake the task of importing rhizome data
 * into the working database
 */
public class ImportData extends AbstractTask {
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	private String tableName = null;
	private File inputFile = null;
	private String tabletId = null;
	private long insertCount = 0;
	
	private Connection sourceConnection = null;
	private Connection destConnection = null;
	
	/**
	 * imports the data from a Rhizome database into the MySQL table
	 * 
	 * Use this constructor when using this class as part of a batch operation
	 * 
	 * @param config a Configuration object with preferences
	 * @param tableName the name of the table for the deployment
	 */
	public ImportData(Configuration config, String tableName) {
		
		// validate the parameters
		if(config == null) {
			throw new IllegalArgumentException("config is a required parameter");
		}
		
		if(StringUtils.isEmpty(tableName) == true) {
			throw new IllegalArgumentException("the table name is required");
		}
		
		this.config = config;
		this.tableName = tableName;
		
	}
	
	/**
	 * imports the data from a Rhizome database into the MySQL table
	 * 
	 * @param config a Configuration object with preferences
	 * @param tableName the name of the table for the deployment
	 * @param inputFile the Rhizome database file
	 * @param tabletId the unique id of the tablet
	 */
	public ImportData(Configuration config, String tableName, File inputFile, String tabletId) {
		
		// validate the parameters
		if(config == null) {
			throw new IllegalArgumentException("config is a required parameter");
		}
		
		if(StringUtils.isEmpty(tableName) == true) {
			throw new IllegalArgumentException("the table name is required");
		}
		
		if(inputFile == null) {
			throw new IllegalArgumentException("the input file is required");
		}
		
		if(StringUtils.isEmpty(tabletId) == true) {
			throw new IllegalArgumentException("the tablet id is required");
		}
		
		this.config = config;
		this.tableName = tableName;
		this.inputFile = inputFile;
		this.tabletId = tabletId;
	}
	
	/**
	 * undertake the task of importing the data
	 */
	public void doTask() throws TaskException {

		// get a connection to the source database
		Statement sourceStatement = null;
		ResultSet sourceResultSet = null;
		
		//reset the insert count
		insertCount = 0;
		
		if(sourceConnection != null) {
			try {
				sourceConnection.close();
				sourceConnection = null;
			} catch (SQLException e){
				throw new TaskException("unable to close existing connection to a rhizome database");
			}
		}
		
		try {
			sourceConnection = DriverManager.getConnection("jdbc:sqlite:" + inputFile.getCanonicalPath());
			sourceStatement = sourceConnection.createStatement();
			sourceStatement.setQueryTimeout(30);
		} catch (SQLException e) {
			throw new TaskException("unable to open connection to the Rhizome database", e);
		} catch (IOException e) {
			throw new TaskException("unable to open connection to the Rhizome database", e);
		}
		
		// get the data
		try {
			sourceResultSet = sourceStatement.executeQuery("select id, name, author, inserttime, filesize from manifests;");
		} catch (SQLException e) {
			throw new TaskException("unable to query the database", e);
		}
		
		// get a connection to the destination database if required
		if(destConnection == null) {
			try {
				destConnection = DatabaseUtils.getMysqlConnection(config);
			} catch (SQLException e) {
				throw new TaskException("unable to open connection to the MySQL database", e);
			}
			
			// check if the table already exists
			try {
				if(DatabaseUtils.doesTableExist(destConnection, tableName) == false) {
					throw new TaskException("the specified table '" + tableName + "' doesn't exist");
				}
			} catch (SQLException e) {
				throw new TaskException("unable to communicate with the the database:\n" + e.getMessage());
			}
		}
		
		String sql = "INSERT INTO " + tableName + " (tablet_id, file_id, file_name, file_author_sid, file_insert_time, file_size) VALUES (?,?,?,?,?,?)";
		
		// define a prepared statement
		PreparedStatement destStatement = null;
		
		try {
			destStatement = destConnection.prepareStatement(sql);
		} catch (SQLException e) {
			throw new TaskException("unable to create insert statement", e);
		}
		
		// import the data
		try {
			while (sourceResultSet.next() == true) {
				
				destStatement.setString(1, tabletId);
				destStatement.setString(2, sourceResultSet.getString("id"));
				destStatement.setString(3, sourceResultSet.getString("name"));
				destStatement.setString(4, sourceResultSet.getString("author"));
				destStatement.setLong(5, sourceResultSet.getLong("inserttime"));
				destStatement.setLong(6, sourceResultSet.getLong("filesize"));
				destStatement.executeUpdate();
				
				insertCount++;
			}
		} catch (SQLException e) {
			throw new TaskException("error in inserting data: '" + e.getMessage());
		}
	}
	
	/**
	 * play nice and close any database related connections and resources
	 * 
	 * @throws SQLException if something bad happens
	 */
	public void closeConnection() throws SQLException {
		
		if(sourceConnection != null) {
			sourceConnection.close();
		}
		
		if(destConnection != null) {
			destConnection.close();
		}
		
	}
	
	/**
	 * return the number of records inserted into the database
	 * 
	 * @return the number of records inserted into the database
	 */
	public long getInsertCount() {
		return insertCount;
	}
	
	/**
	 * set the path to the input file
	 * @param inputFile the path to the Rhizome database file
	 */
	public void setInputFile(File inputFile) {
		
		if(inputFile == null) {
			throw new IllegalArgumentException("the input file is required");
		}
		
		this.inputFile = inputFile;	
	}
	
	/**
	 * set the id of the tablet that was the source of this data
	 * 
	 * @param tabletId the unique tabletid
	 */
	public void setTableId(String tabletId) {
		
		if(StringUtils.isEmpty(tabletId) == true) {
			throw new IllegalArgumentException("the tablet id is required");
		}
		
		this.tabletId = tabletId;
	}

}
