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
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.configuration.Configuration;
import org.magdaaproject.utils.DatabaseUtils;
import org.magdaaproject.utils.StringUtils;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * calculate the data for the bundles over time spreadsheet
 */
public class BundlesOverTime extends AbstractTask {
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	private String tableName;
	
	private Connection connection = null;
	
	private File outputFile;
	
	private String[] values = new String[4];
	private String[] headers = {"file_id", "tablet_id", "timestamp", "count"};
	
	/**
	 * create the class
	 * @param config a Configuration object with details about the MySQL database
	 * @param tableName the name of the table to use for the analysis
	 * @param outputFile the name of the output file
	 */
	public BundlesOverTime(Configuration config, String tableName, File outputFile) {
		
		// validate the parameters
		if(config == null) {
			throw new IllegalArgumentException("config is a required parameter");
		}
		
		if(StringUtils.isEmpty(tableName) == true) {
			throw new IllegalArgumentException("the table name is required");
		}
		
		if(outputFile == null) {
			throw new IllegalArgumentException("outputFile is a required parameter");
		}
		
		this.config = config;
		this.tableName = tableName;
		this.outputFile = outputFile;
	}

	/**
	 * undertake the generation of the csv file for this graph
	 */
	@Override
	public void doTask() throws TaskException {
		
		// get a connection to the database
		try {
			connection = DatabaseUtils.getMysqlConnection(config);
		} catch (SQLException e) {
			throw new TaskException("unable to connect to the database:\n" + e.getMessage());
		}
		
		// check if the table already exists
		try {
			if(DatabaseUtils.doesTableExist(connection, tableName) == false) {
				throw new TaskException("the specified table '" + tableName + "' doesn't exist");
			}
		} catch (SQLException e) {
			throw new TaskException("unable to communicate with the the database:\n" + e.getMessage());
		}
		
		// get the data
		String sql = "SELECT file_id, tablet_id, file_insert_time FROM " + tableName + " WHERE file_insert_time IS NOT NULL ORDER BY file_id, file_insert_time ASC";
		Statement statement = null;
		ResultSet resultSet = null;
		
		int fileCount = 0;
		String currentFileId = "";
		
		try {
			 statement = connection.createStatement();
		} catch (SQLException e) {
			throw new TaskException("unable to create statement '" + sql + "': " + e.getMessage());
		}
		
		try {
			resultSet = statement.executeQuery(sql);
		} catch (SQLException e) {
			throw new TaskException("unable to execute statement '" + sql + "': " + e.getMessage());
		}
		
		// open the output file
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(outputFile), ',');
			
			// output the header information
			writer.writeNext(headers);
			
		} catch (IOException e) {
			throw new TaskException("unable to open output file '" + sql + "': " + e.getMessage());
		}
		
		try {
			
			// loop through the data
			while(resultSet.next() == true) {
				
				if(currentFileId.equals(resultSet.getString(1)) == false) {
					
					// start a new count
					currentFileId = resultSet.getString(1);
					fileCount = 1;
					values[0] = resultSet.getString(1);
					values[1] = resultSet.getString(2);
					values[2] = resultSet.getString(3);
					values[3] = Integer.toString(fileCount);
					
					writer.writeNext(values);
					
				} else {
					// continue an existing count
					fileCount++;
					values[2] = resultSet.getString(3);
					values[3] = Integer.toString(fileCount);
					
					writer.writeNext(values);
				}
				
			}
			
		} catch (SQLException e) {
			throw new TaskException("unable to get results of sql query '" + sql + "': " + e.getMessage());
		} finally {
			try {
				resultSet.close();
				statement.close();
			} catch (SQLException e) {
				throw new TaskException("unable to clean up database resources: \n" + e.getMessage());
			}
			
			try {
				writer.close();
			} catch (IOException e) {
				throw new TaskException("unable to close the output file: \n" + e.getMessage());
			}
		}

	}
	
	/**
	 * close the database connection
	 * 
	 * @throws SQLException if something bad happens
	 */
	public void closeConnection() throws SQLException {
		if(connection != null) {
			connection.close();
		}
	}

}
