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

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.configuration.Configuration;
import org.magdaaproject.utils.DatabaseUtils;
import org.magdaaproject.utils.StringUtils;

import java.sql.Connection;


/**
 * Undertake a task to create a database table
 */
public class CreateTable extends AbstractTask {
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	private String tableName;
	
	private Connection connection = null;
	
	/**
	 * create a table representing a deployment
	 * 
	 * @param config a Configuration object with preferences
	 * @param tableName the name of the table
	 */
	public CreateTable(Configuration config, String tableName) {
		
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
	 * undertake the task to create a table
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
			if(DatabaseUtils.doesTableExist(connection, tableName) == true) {
				throw new TaskException("the specified table '" + tableName + "' already exists");
			}
		} catch (SQLException e) {
			throw new TaskException("unable to communicate with the the database:\n" + e.getMessage());
		}
		
		// create the table
		Statement createStatement;
		try {
			 createStatement = connection.createStatement();
			 
			 // create the table
			 String sql = "CREATE TABLE " + tableName + " ("
					 + "id BIGINT NOT NULL auto_increment, "
					 + "tablet_id VARCHAR(10) NOT NULL, "
					 + "file_id VARCHAR(70) NOT NULL, "
					 + "file_name VARCHAR(250) NOT NULL, "
					 + "file_author_sid VARCHAR(70), "
					 + "file_insert_time BIGINT, "
					 + "file_size BIGINT NOT NULL, "
					 + "origin CHAR(1) NOT NULL DEFAULT '" + DatabaseUtils.DATABASE_CONST_NO + "', "
					 + "PRIMARY KEY(id)) CHARACTER SET 'utf8'";
			 
			 createStatement.executeUpdate(sql);
			 
			 // create the indexes
			 sql = "CREATE INDEX " + tableName + "_tablet_id" + " ON " + tableName + "(tablet_id ASC)";
			 createStatement.executeUpdate(sql);
			 
			 sql = "CREATE INDEX " + tableName + "_file_id" + " ON " + tableName + "(file_id)";
			 createStatement.executeUpdate(sql);
			 
			 sql = "CREATE INDEX " + tableName + "_file_author_sid" + " ON " + tableName + "(file_author_sid)";
			 createStatement.executeUpdate(sql);
			 
			 sql = "CREATE INDEX " + tableName + "_file_insert_time" + " ON " + tableName + "(file_insert_time)";
			 createStatement.executeUpdate(sql);
			 
			 sql = "CREATE INDEX " + tableName + "_file_size" + " ON " + tableName + "(file_size)";
			 createStatement.executeUpdate(sql);
			 
			 sql = "CREATE INDEX " + tableName + "_file_name" + " ON " + tableName + "(file_name)";
			 createStatement.executeUpdate(sql);
			 
			 sql = "CREATE INDEX " + tableName + "_origin" + " ON " + tableName + "(origin)";
			 createStatement.executeUpdate(sql);
			 
			 // play nice and tidy up
			 createStatement.close();
			 
		} catch (SQLException e) {
			throw new TaskException("unable to create the table '" + tableName + "':\n" + e.getMessage());
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
