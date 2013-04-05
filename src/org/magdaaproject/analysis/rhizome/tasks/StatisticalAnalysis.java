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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.magdaaproject.utils.DatabaseUtils;
import org.magdaaproject.utils.StringUtils;

/**
 * a class to undertake the generation of statistics
 */
public class StatisticalAnalysis extends AbstractTask {
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	private String tableName;
	
	private Connection connection = null;
	
	private StringBuilder output = null;
	
	/**
	 * generate statistics from the data in a specified table
	 * 
	 * @param config
	 * @param tableName
	 */
	public StatisticalAnalysis(Configuration config, String tableName) {
		
		// validate the parameters
		if(config == null) {
			throw new IllegalArgumentException("config is a required parameter");
		}
		
		if(StringUtils.isEmpty(tableName) == true) {
			throw new IllegalArgumentException("the table name is required");
		}
		
		this.config = config;
		this.tableName = tableName;
		
		output = new StringBuilder("\nStatistical Analysis for table: " + tableName + "\n");
	}

	/**
	 * generate the statistics
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
		
		/*
		 * gather the statistics
		 */
		
		// total number of files in Rhizome
		output.append("Total unique files on the mesh: " + getTotalUniqueFileCount());
		output.append("Total bundles on the mesh: " + getTotalBundleCount());
		output.append("Total unique data size on the mesh: " + getTotalUniqueDataSize());
		output.append("Total data size (including duplicates) on the mesh: " + getTotalDataSize());
		output.append("Average file size: " + getAverageFileSize());
		output.append("Average number of bundles per device: " + getAverageBundlesPerDevice());
		output.append("Total number of files without resilient copies: " + getFilesWithoutResilientCopies());
		output.append("Total number of files with resilient copies: " + getFilesWithResilientCopies());
		output.append("Maximum resilient copy count: " + getMaxResilientCopyCount());
		output.append("Minimum resilient copy count: " + getMinResilientCopyCount());
		output.append("Approximate Maximum time delay before first resilient copy: " + getMaxTimeDelayBeforeFirstCopy());
		output.append("Approximate Minimum time delay before first resilient copy: " + getMinTimeDelayBeforeFirstCopy());
		output.append("Total number of files not on the laptop: " + getFilesNotOnLaptop());
		
		// print the statistics
		System.out.println(output.toString());

	}
	
	// method to get the total file count
	private String getTotalUniqueFileCount() throws TaskException {
		return executeSql("SELECT COUNT(DISTINCT file_id) FROM " + tableName) + "\n";
	}
	
	private String getTotalBundleCount() throws TaskException {
		return executeSql("SELECT COUNT(file_id) FROM " + tableName) + "\n";
	}
	
	// method to get the total unique data size
	private String getTotalUniqueDataSize() throws TaskException {
		String totalFileSize = executeSql("SELECT SUM(file_size) FROM (SELECT file_id, file_size FROM " + tableName + " GROUP BY file_id) as TABLE_01");
		
		totalFileSize = FileUtils.byteCountToDisplaySize(Long.parseLong(totalFileSize));
		
		return totalFileSize + "\n";
	}
	
	// method to get the total data size including duplicate copies
	private String getTotalDataSize() throws TaskException {
		
		String totalFileSize = executeSql("SELECT SUM(file_size) FROM " + tableName);
		
		totalFileSize = FileUtils.byteCountToDisplaySize(Long.parseLong(totalFileSize));
		
		return totalFileSize + "\n";
	}
	
	// method to get the average file size
	private String getAverageFileSize() throws TaskException {
		
		String averageFileSize = executeSql("SELECT AVG(file_size) FROM (SELECT file_id, file_size FROM " + tableName + " GROUP BY file_id) AS table_01");
		
		averageFileSize = FileUtils.byteCountToDisplaySize(Math.round(Double.parseDouble(averageFileSize)));
		
		return averageFileSize + "\n";
	}
	
	// method to get the average number of bundles per tablet
	private String getAverageBundlesPerDevice() throws TaskException {
		
		String value = executeSql("SELECT AVG(file_ids) FROM (SELECT tablet_id, COUNT(file_id) as FILE_IDS FROM " + tableName + " GROUP BY tablet_id) AS table_01;");
		
		return value + "\n";
	}
	
	// number of files without a resilient copy
	private String getFilesWithoutResilientCopies() throws TaskException {
		int value = executeSqlForRowCount("SELECT file_id, COUNT(file_id) FROM " + tableName + " GROUP BY file_id HAVING COUNT(file_id) = 1");
		return Integer.toString(value) + "\n";
	}
	
	// number of files with a resilient copy
	private String getFilesWithResilientCopies() throws TaskException {
		int value = executeSqlForRowCount("SELECT file_id, COUNT(file_id) FROM " + tableName + " GROUP BY file_id HAVING COUNT(file_id) > 1");
		return Integer.toString(value) + "\n";
	}
	
	// maximum number of resilient copies
	private String getMaxResilientCopyCount() throws TaskException {
		return executeSql("SELECT MAX(file_id_count) FROM (SELECT file_id, COUNT(file_id) AS file_id_count FROM " + tableName + " GROUP BY file_id) as TABLE_01;") + "\n";
	}
	
	// minimum number of resilient copies
	private String getMinResilientCopyCount() throws TaskException {
		return executeSql("SELECT MIN(file_id_count) FROM (SELECT file_id, COUNT(file_id) AS file_id_count FROM " + tableName + " GROUP BY file_id HAVING COUNT(file_id) > 1) as TABLE_01;") + "\n";
				
	}
	
	// maximum time delay before first copy
	private String getMaxTimeDelayBeforeFirstCopy() throws TaskException {
		StringBuilder builder = new StringBuilder();
		
		builder.append("SELECT MAX(time_difference) ");
		builder.append("FROM (SELECT " + tableName + ".file_id, MIN(" + tableName + ".file_insert_time), table_01.file_insert_time, MIN(" + tableName + ".file_insert_time) - table_01.file_insert_time AS time_difference ");
		builder.append("FROM " + tableName + ", (SELECT file_id, file_insert_time FROM " + tableName + " WHERE origin = 'y' AND file_insert_time IS NOT NULL) AS table_01 ");
		builder.append("WHERE table_01.file_id = " + tableName + ".file_id ");
		builder.append("AND table_01.file_insert_time <> " + tableName + ".file_insert_time ");
		builder.append("AND " + tableName + ".file_insert_time IS NOT NULL ");
		builder.append("GROUP BY " + tableName + ".file_id) as table_02");
		
		return DurationFormatUtils.formatDuration(Long.parseLong(executeSql(builder.toString())), "H:m:s") + " (H:m:s)\n";
	}
	
	// minimum time delay before first copy
	private String getMinTimeDelayBeforeFirstCopy() throws TaskException {
		StringBuilder builder = new StringBuilder();
		
		builder.append("SELECT MIN(time_difference) ");
		builder.append("FROM (SELECT " + tableName + ".file_id, MIN(" + tableName + ".file_insert_time), table_01.file_insert_time, MIN(" + tableName + ".file_insert_time) - table_01.file_insert_time AS time_difference ");
		builder.append("FROM " + tableName + ", (SELECT file_id, file_insert_time FROM " + tableName + " WHERE origin = 'y' AND file_insert_time IS NOT NULL) AS table_01 ");
		builder.append("WHERE table_01.file_id = " + tableName + ".file_id ");
		builder.append("AND table_01.file_insert_time <> " + tableName + ".file_insert_time ");
		builder.append("AND " + tableName + ".file_insert_time IS NOT NULL ");
		builder.append("GROUP BY " + tableName + ".file_id) as table_02 ");
		builder.append("WHERE time_difference > 0");
		
		return DurationFormatUtils.formatDuration(Long.parseLong(executeSql(builder.toString())), "H:m:s") + " (H:m:s)\n";
	}
	
	// number of files not on the laptop
	private String getFilesNotOnLaptop() throws TaskException {
		return executeSql("SELECT COUNT(file_id) FROM " + tableName + " WHERE file_id NOT IN (SELECT file_id FROM " + tableName + " WHERE tablet_id = 'laptop')") + "\n";
	}
	/*
	 * sql execute methods
	 */
	
	// method to execute an SQL and return the single return value
	private String executeSql(String sql) throws TaskException {
		
		if(StringUtils.isEmpty(sql) == true) {
			throw new IllegalArgumentException("the sql parameter is required");
		}
		
		String returnValue = null;
		Statement statement = null;
		
		try {
			 statement = connection.createStatement();
		} catch (SQLException e) {
			throw new TaskException("unable to create statement '" + sql + "': " + e.getMessage());
		}
		
		ResultSet resultSet = null;
		
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
	
	// method to execute an SQL and return the single return value
	private int executeSqlForRowCount(String sql) throws TaskException {
		
		if(StringUtils.isEmpty(sql) == true) {
			throw new IllegalArgumentException("the sql parameter is required");
		}
		
		int returnValue = 0;
		Statement statement = null;
		
		try {
			 statement = connection.createStatement();
		} catch (SQLException e) {
			throw new TaskException("unable to create statement '" + sql + "': " + e.getMessage());
		}
		
		ResultSet resultSet = null;
		
		try {
			resultSet = statement.executeQuery(sql);
		} catch (SQLException e) {
			throw new TaskException("unable to execute statement '" + sql + "': " + e.getMessage());
		}
		
		try {
			
			resultSet.last();
			returnValue = resultSet.getRow();

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
