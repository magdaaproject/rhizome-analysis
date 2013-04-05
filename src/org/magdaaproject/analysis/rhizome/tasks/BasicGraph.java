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
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.magdaaproject.utils.DatabaseUtils;
import org.magdaaproject.utils.StringUtils;

/**
 * a class to undertake the task of generating a network graph
 * 
 * Deprecated until such time as sender information is included in Rhizome
 */
@Deprecated
public class BasicGraph extends AbstractTask {
	
	/*
	 * public class level constants
	 */
	/**
	 * the default sync window, in seconds
	 */
	public static final long DEFAULT_SYNC_WINDOW = 30;
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	private String tableName;
	private File outputFile;
	private long syncWindowPeriod;
	
	private Connection connection = null;
	
	public BasicGraph(Configuration config, String tableName, File outputFile, Long syncWindow) {
		
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
		
		if(syncWindow == null) {
			this.syncWindowPeriod = DEFAULT_SYNC_WINDOW * 1000;
		} else {
			this.syncWindowPeriod = syncWindow * 1000;
		}
	}
	
	/**
	 * undertake the task of generating the graph
	 */
	@Override
	public void doTask() throws TaskException {
		
		// get a database connection
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
		
		// get the file id of the most resilient file
		String fileId = getFileId();
		
		String originTablet = getOriginTablet(fileId);
		
		String[] tmpString = originTablet.split("\\|");
		long tmpLong;
		
		originTablet = tmpString[0];
		long originInsertTime = Long.parseLong(tmpString[1]);
		
		ArrayList<String> copies = getResilientCopies(fileId);
		
		int clusterCount = 0;
		
		StringBuilder builder = new StringBuilder();
		
		// build the contents of the dot output file
		builder.append("digraph magdaa_graph {\n");
		builder.append("\t graph [compound=true];\n");
		builder.append("\t node [color=lightblue2, style=filled];\n");
		
		// start the first subgraph
		builder.append("\t subgraph cluster_" + clusterCount + " {\n");
		builder.append("\t\t label = \"cluster " + clusterCount + "\";\n");
		builder.append("\t\t color=blue;\n");
		
		// add the sub graphs
		for(String tabletId: copies) {
			tmpString = tabletId.split("\\|");
			tabletId = tmpString[0];
			tmpLong = Long.parseLong(tmpString[1]);
			
			// add to existing sub graph or start a new one
			if(tmpLong < originInsertTime + syncWindowPeriod) {
				// add to existing sub graph
				builder.append("\t\t \"" + tabletId + "\";\n");
			} else {
				// start and add to a new sub graph
				builder.append("\t}\n");
				clusterCount++;
				builder.append("\t subgraph cluster_" + clusterCount + " {\n");
				builder.append("\t\t label = \"cluster " + clusterCount + "\";\n");
				builder.append("\t\t color=blue;\n");
				builder.append("\t\t \"" + tabletId + "\";\n");
				
				// increase the origin insert time
				originInsertTime = tmpLong;
			}
		}
		
		// finish the final subgraph
		builder.append("\t}\n\n");
		
		// add the link from the origin node to the first group
		String firstCopy = copies.get(0);
		tmpString = firstCopy.split("\\|");
		
		firstCopy = tmpString[0];
		
		builder.append("\t\"" + originTablet + "\" -> \"" + firstCopy + "\" [ltail=\"" + originTablet + "\" lhead=cluster_1];");
		
		
//		// add the main graph
//		for(String tabletId: copies) {
//			tmpString = tabletId.split("\\|");
//			tabletId = tmpString[0];
//			
//			builder.append("\t\"" + originTablet + "\" -> \"" + tabletId + "\";\n");
//		}
		
		builder.append("}");
		
		// output the file
		PrintWriter fileWriter = null;
		
		try {
			fileWriter = new PrintWriter(outputFile, "UTF-8");
		} catch (FileNotFoundException e) {
			throw new TaskException("unable to create the output file:\n" + e.getMessage());
		} catch (UnsupportedEncodingException e) {
			throw new TaskException("unable to create the output file:\n" + e.getMessage());
		}
		
		fileWriter.print(builder.toString());
		
		fileWriter.close();
		
	}
	
	/*
	 * private method to get the file id
	 */
	private String getFileId() throws TaskException{
		
		String sql = "SELECT file_id, COUNT(file_id) FROM " + tableName + " GROUP BY file_id ORDER BY COUNT(file_id) DESC";
		Statement statement = null;
		ResultSet resultSet = null;
		String returnValue = null;
		
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
		
		try {
			if(resultSet.next()) {
				returnValue = resultSet.getString(1);
				
				// play nice and tidy up
				resultSet.close();
				statement.close();
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
		}
		
		return returnValue;
		
	}
	
	private String getOriginTablet(String fileId) throws TaskException {
		
		String sql = "SELECT tablet_id, file_insert_time FROM " + tableName + " WHERE file_id = ? AND origin = ?";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		String returnValue = null;
		
		try {
			 statement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			throw new TaskException("unable to create statement '" + sql + "': " + e.getMessage());
		}
		
		try {
			
			statement.setString(1, fileId);
			statement.setString(2, DatabaseUtils.DATABASE_CONST_YES);
			
			resultSet = statement.executeQuery();
		} catch (SQLException e) {
			throw new TaskException("unable to execute statement '" + sql + "': " + e.getMessage());
		}
		
		try {
			if(resultSet.next()) {
				returnValue = resultSet.getString(1) + "|" + resultSet.getString(2);
				
				// play nice and tidy up
				resultSet.close();
				statement.close();
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
		}
		
		return returnValue;
		
	}
	
	private ArrayList<String> getResilientCopies(String fileId) throws TaskException {
		String sql = "SELECT tablet_id, file_insert_time FROM " + tableName + " WHERE file_id = ? AND origin = ? and file_insert_time IS NOT NULL ORDER BY file_insert_time ASC";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		ArrayList<String> returnValue = new ArrayList<String>();
		
		try {
			 statement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			throw new TaskException("unable to create statement '" + sql + "': " + e.getMessage());
		}
		
		try {
			
			statement.setString(1, fileId);
			statement.setString(2, DatabaseUtils.DATABASE_CONST_NO);
			
			resultSet = statement.executeQuery();
		} catch (SQLException e) {
			throw new TaskException("unable to execute statement '" + sql + "': " + e.getMessage());
		}
		
		try {
			while(resultSet.next()) {
				returnValue.add(resultSet.getString(1) + "|" + resultSet.getString(2));
			}
			
			// play nice and tidy up
			resultSet.close();
			statement.close();
			
		} catch (SQLException e) {
			throw new TaskException("unable to get results of sql query '" + sql + "': " + e.getMessage());
		} finally {
			try {
				resultSet.close();
				statement.close();
			} catch (SQLException e) {
				throw new TaskException("unable to clean up database resources: \n" + e.getMessage());
			}
		}
		
		return returnValue;
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
