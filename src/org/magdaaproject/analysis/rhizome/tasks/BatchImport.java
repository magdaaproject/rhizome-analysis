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
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.magdaaproject.utils.DatasetDirectoryWalker;
import org.magdaaproject.utils.StringUtils;

/**
 * a class which implements the batch import task
 */
public class BatchImport extends AbstractTask {
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	private String tableName;
	private File inputDir;
	
	private long totalCount;
	
	/**
	 * imports the data from a batch of Rhizome databases into the MySQL table
	 * 
	 * @param config a Configuration object with preferences
	 * @param tableName the name of the table for the deployment
	 * @param inputDir the path to the parent directory of the dataset
	 * @param tabletId the unique id of the tablet
	 */
	public BatchImport(Configuration config, String tableName, File inputDir) {
		
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
	 * undertake the task of importing an entire dataset as a batch
	 */
	@Override
	public void doTask() throws TaskException {
		
		// get a list of rhizome files to process
		DatasetDirectoryWalker rhizomeFileFinder = new DatasetDirectoryWalker(
			HiddenFileFilter.VISIBLE,
		    FileFilterUtils.nameFileFilter("rhizome.db")
		);
		
		ArrayList<File> rhizomeFiles = null;
		
		try {
			rhizomeFiles = rhizomeFileFinder.getFileList(inputDir);
		} catch (IOException e) {
			throw new TaskException("unable to gather a list of rhizome databases: \n" + e.getMessage());
		}
		
		if(rhizomeFiles.size() == 0) { 
			throw new TaskException("unable to locate any rhizome database files");
		}
		
		// declare helper variables
		ImportData importDataTask = new ImportData(config, tableName);
		
		// loop through all of the available files
		for(File rhizomeFile : rhizomeFiles) {
			System.out.println("Importing data from:");
			System.out.println(rhizomeFile.getAbsolutePath());
			
			importDataTask.setInputFile(rhizomeFile);
			importDataTask.setTableId(rhizomeFile.getParentFile().getName());
			
			try {
				importDataTask.doTask();
			} catch (TaskException e) {
				throw new TaskException("unable to import data from\n '" + rhizomeFile.getAbsolutePath() + "\n" + e.getMessage());
			}
			
			System.out.println("SUCCESS: " + importDataTask.getInsertCount() + " records added successfully");
			
			totalCount += importDataTask.getInsertCount();
			
		}
		
		// close the connection
		try {
			importDataTask.closeConnection();
		} catch (SQLException e) {
			throw new TaskException("unable to close MySQL connection:\n" + e.getMessage());
		}
	}
	
	/**
	 * return the number of records inserted into the database
	 * 
	 * @return the number of records inserted into the database
	 */
	public long getTatalCount() {
		return totalCount;
	}
}
