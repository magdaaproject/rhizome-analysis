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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.magdaaproject.utils.DatabaseUtils;
import org.magdaaproject.utils.DatasetDirectoryWalker;
import org.magdaaproject.utils.StringUtils;

/**
 * class to update the origin column of the data table
 */
public class UpdateOrigin extends AbstractTask {
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	private String tableName;
	private File inputDir;
	
	private long totalCount;
	private Connection connection = null;
	
	private String fileNameSuffix = ".instance.sam.magdaa";
	
	/**
	 * imports the data from a batch of Rhizome databases into the MySQL table
	 * 
	 * @param config a Configuration object with preferences
	 * @param tableName the name of the table for the deployment
	 * @param inputDir the path to the parent directory of the dataset
	 * @param tabletId the unique id of the tablet
	 */
	public UpdateOrigin(Configuration config, String tableName, File inputDir) {
		
		// validate the parameters
		if(config == null) {
			throw new IllegalArgumentException("config is a required parameter");
		}
		
		if(StringUtils.isEmpty(tableName) == true) {
			throw new IllegalArgumentException("the table name is required");
		}
		
		if(inputDir == null) {
			throw new IllegalArgumentException("the input parent directory is required");
		}
		
		this.config = config;
		this.tableName = tableName;
		this.inputDir = inputDir;
		totalCount = 0;
	}
	

	/**
	 * do the task of updating the origin attribute for files
	 */
	@Override
	public void doTask() throws TaskException {
		
		// get a list of rhizome files to process
		DatasetDirectoryWalker rhizomeFileFinder = new DatasetDirectoryWalker(
			HiddenFileFilter.VISIBLE,
			FileFilterUtils.suffixFileFilter(".xml")
		);
		
		ArrayList<File> surveyFiles = null;
		
		try {
			surveyFiles = rhizomeFileFinder.getFileList(inputDir);
		} catch (IOException e) {
			throw new TaskException("unable to gather a list of rhizome databases: \n" + e.getMessage());
		}
		
		if(surveyFiles.size() == 0) { 
			throw new TaskException("unable to locate any rhizome database files");
		}
		
		// declare helper variables
		String fileName = null;
		String tabletId = null;
		int    updateCount = 0;
		
		// get a connection to the destination database if required
		if(connection == null) {
			try {
				connection = DatabaseUtils.getMysqlConnection(config);
			} catch (SQLException e) {
				throw new TaskException("unable to open connection to the MySQL database", e);
			}
			
			// check if the table already exists
			try {
				if(DatabaseUtils.doesTableExist(connection, tableName) == false) {
					throw new TaskException("the specified table '" + tableName + "' doesn't exist");
				}
			} catch (SQLException e) {
				throw new TaskException("unable to communicate with the the database:\n" + e.getMessage());
			}
		}
		
		String sql = "UPDATE " + tableName + " SET origin = ? WHERE tablet_id = ? AND file_name = ?";
		
		// define a prepared statement
		PreparedStatement statement = null;
		
		try {
			statement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			throw new TaskException("unable to create update statement", e);
		}
		
		// loop through the list of files
		try {
			for(File surveyFile: surveyFiles) {
				
				// get the file name and tablet id
				fileName = surveyFile.getName() + fileNameSuffix;
				tabletId = surveyFile.getParentFile().getParentFile().getName();
				
				statement.setString(1, DatabaseUtils.DATABASE_CONST_YES);
				statement.setString(2, tabletId);
				statement.setString(3, fileName);
				
				updateCount = statement.executeUpdate();
				
				if(updateCount == 1) {
					totalCount++;
				} else {
					System.err.println("Found file not in rhizome:");
					System.err.println(surveyFile.getAbsolutePath());
				}
			}
		} catch (SQLException e) {
			throw new TaskException("unable to update record for file '" + fileName + "' and tabletid '" + tabletId + "':\n" + e.getMessage(), e);
		}
		
		ArrayList<String> recordsToDelete = new ArrayList<String>(); 
		
		// delete entries for erroneous files
		try {
			statement.close();
			
			sql = "SELECT DISTINCT file_id FROM " + tableName + " WHERE file_id NOT IN (SELECT file_id FROM " + tableName + " WHERE origin = ?)";
			statement = connection.prepareStatement(sql);
			statement.setString(1, DatabaseUtils.DATABASE_CONST_YES);
			
			ResultSet resultSet = statement.executeQuery();
			
			while(resultSet.next()) {
				recordsToDelete.add(resultSet.getString(1));
			}
			
			resultSet.close();
			statement.close();
			
			sql = "DELETE FROM " + tableName + " WHERE file_id = ?";
			statement = connection.prepareStatement(sql);
			
			for(String recordToDelete: recordsToDelete) {

				statement.setString(1, recordToDelete);
				updateCount = statement.executeUpdate();
				
				if(updateCount > 0) {
					System.out.println("Deleted erroneous record: '" + recordToDelete + "'");
				} else {
					System.out.println("Unable to delete erroneouns record: '" + recordToDelete + "'");
				}
				
			}
		}	catch (SQLException e) {
			throw new TaskException("unable to delete erronous records:\n" + e.getMessage(), e);
		}

	}
	
	/**
	 * return the number of records inserted into the database
	 * 
	 * @return the number of records inserted into the database
	 */
	public long getUpdateCount() {
		return totalCount;
	}
	
	/**
	 * play nice and close any database related connections and resources
	 * 
	 * @throws SQLException if something bad happens
	 */
	public void closeConnection() throws SQLException {
		
		if(connection != null) {
			connection.close();
		}
		
	}

}
