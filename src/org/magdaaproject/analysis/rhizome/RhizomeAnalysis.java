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
package org.magdaaproject.analysis.rhizome;

import java.io.File;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.magdaaproject.analysis.rhizome.tasks.BatchImport;
import org.magdaaproject.analysis.rhizome.tasks.BundlesOverTime;
import org.magdaaproject.analysis.rhizome.tasks.CreateTable;
import org.magdaaproject.analysis.rhizome.tasks.ImportData;
import org.magdaaproject.analysis.rhizome.tasks.StatisticalAnalysis;
import org.magdaaproject.analysis.rhizome.tasks.TaskException;
import org.magdaaproject.analysis.rhizome.tasks.UpdateOrigin;
import org.magdaaproject.analysis.rhizome.tasks.ValidateProperties;
import org.magdaaproject.utils.StringUtils;

/**
 * main entry point to the Rhizome Analysis application
 *
 */
public class RhizomeAnalysis {

	/*
	 * public constants
	 */
	/**
	 * name of the application
	 */
	public static final String APP_NAME = "MaGDAA Rhizome Analysis";

	/**
	 * version number of the application
	 */
	public static final String APP_VERSION = "1.0";

	/**
	 * URL for more information
	 */
	public static final String MORE_INFO = "http://magdaaproject.org";

	/**
	 * URL for the license information
	 */
	public static final String LICENSE_INFO = "http://www.gnu.org/copyleft/gpl.html";

	/**
	 * list of valid task types
	 */

	public static final String[] TASK_TYPES = {"create-table", "import-data", "batch-import", "update-origin", "statistics", "chart-bundles-over-time"};

	/*
	 * private class level variables
	 */
	private static CommandLine cmd = null;
	private static Configuration config = null;

	/**
	 * main method
	 */
	public static void main(String[] args) {

		//output some text
		System.out.println(APP_NAME + " - " + APP_VERSION);

		/*
		 * load database related classes
		 */
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			System.err.println("FATAL ERROR: unable to load SQLite classes:\n" + e.getMessage());
			System.exit(-1);
		}

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println("FATAL ERROR: unable to load MySQL classes:\n" + e.getMessage());
			System.exit(-1);
		}

		/*
		 *  parse the command line options
		 */
		CommandLineParser parser = new PosixParser();
		try {
			cmd = parser.parse(createOptions(), args);
		}catch(org.apache.commons.cli.ParseException e) {
			// something bad happened so output help message
			printCliHelp("Error in parsing arguments:\n" + e.getMessage());
		}

		/*
		 * get and test the command line parameters
		 */

		// task type
		String taskType = cmd.getOptionValue("task");

		if(StringUtils.isEmpty(taskType) == true) {
			printCliHelp("ERROR: the task type is required");
		}

		if(StringUtils.isInArray(taskType, TASK_TYPES) == false) {
			printCliHelp("ERROR: unrecognised task type");
		}

		// properties file path
		String propertiesPath = cmd.getOptionValue("properties");

		if(StringUtils.isEmpty(propertiesPath) == true) {
			printCliHelp("ERROR: the path to the properties file is required");
		}

		File propertiesFile = new File(propertiesPath);

		if(propertiesFile.isFile() == false || propertiesFile.canRead() == false) {
			printCliHelp("ERROR: unable to access the properties file");
		}

		// load the properties file
		try {
			config = new PropertiesConfiguration(propertiesFile.getAbsolutePath());
		} catch (ConfigurationException e) {
			printCliHelp("ERROR: unable to access the properties file.\n"+ e.getMessage());
		}

		// validate the properties file
		ValidateProperties propertiesTask = new ValidateProperties(config);

		try {
			propertiesTask.doTask();
		} catch (TaskException e) {
			System.err.println("ERROR: unable to parse properties file:" + "\n" + e.getMessage());
			System.exit(-1);
		}

		/*
		 * work out which task to undertake
		 * 
		 * individual tasks methods are responsible for exiting the application
		 */
		if(taskType.equals("create-table") == true) {
			doCreateTableTask();
		}
		
		if(taskType.equals("import-data") == true) {
			doImportDataTask();
		}
		
		if(taskType.equals("batch-import") == true) {
			doBatchImportTask();
		}
		
		if(taskType.equals("update-origin") == true) {
			doUpdateOriginTask();
		}
		
		if(taskType.equals("statistics") == true) {
			doStatisticsTask();
		}
		
		if(taskType.equals("chart-bundles-over-time") == true) {
			doBundlesOverTimeTask();
		}
	}

	/*
	 * execute the various tasks
	 */
	private static void doCreateTableTask() {

		// we need to create a table so get the name of the table
		String tableName = cmd.getOptionValue("table");

		if(StringUtils.isEmpty(tableName) == true) {
			printCliHelp("ERROR: the table name is required");
		}

		CreateTable createTableTask = new CreateTable(config, tableName);

		//create the table
		try {
			createTableTask.doTask();
		} catch (TaskException e) {
			System.err.println("ERROR: unbable to create the table:" + "\n" + e.getMessage());
			System.exit(-1);
		} finally {
			try {
				createTableTask.closeConnection();
			} catch (SQLException e) {
				System.err.println("ERROR: unable to close the database connection");
				System.exit(-1);
			}
		}

		System.out.println("SUCCESS: the table '" + tableName + "' was created successfully!");
		System.exit(0);

	}
	
	/*
	 * import the data from a rhizome database into our database
	 */
	private static void doImportDataTask() {
		
		// we need to create a table so get the name of the table
		String tableName = cmd.getOptionValue("table");
	
		if(StringUtils.isEmpty(tableName) == true) {
			printCliHelp("ERROR: the table name is required");
		}
		
		// input file
		String inputPath = cmd.getOptionValue("input");
		
		if(StringUtils.isEmpty(inputPath) == true) {
			printCliHelp("ERROR: the path to the rhizome database is required");
		}
		
		File inputFile = new File(inputPath);
		
		if(inputFile.isFile() == false || inputFile.canRead() == false) {
			printCliHelp("ERROR: unable to access the rhizome database file");
		}
		
		// tablet id
		String tabletId = cmd.getOptionValue("tablet");
		
		if(StringUtils.isEmpty(tabletId) == true) {
			printCliHelp("ERROR: the tablet id number is required");
		}
		
		// import the data
		ImportData importDataTask = new ImportData(config, tableName, inputFile, tabletId);
		
		//undertake the task
		try {
			importDataTask.doTask();
		} catch (TaskException e) {
			System.err.println("ERROR: unable to complete data import:" + "\n" + e.getMessage());
			System.exit(-1);
		} finally {
			try {
				importDataTask.closeConnection();
			} catch (SQLException e) {
				System.err.println("ERROR: unable to close the database connection");
				System.exit(-1);
			}
		}

		System.out.println("SUCCESS: " + importDataTask.getInsertCount() + " records added successfully");
		System.exit(0);
	}
	
	/*
	 * undertake the batch import task
	 */
	private static void doBatchImportTask() {
		
		// we need to use a table so get the name of the table
		String tableName = cmd.getOptionValue("table");
	
		if(StringUtils.isEmpty(tableName) == true) {
			printCliHelp("ERROR: the table name is required");
		}
		
		// input file
		String inputPath = cmd.getOptionValue("dataset");
		
		if(StringUtils.isEmpty(inputPath) == true) {
			printCliHelp("ERROR: the path to the rhizome database is required");
		}
		
		File inputFile = new File(inputPath);
		
		if(inputFile.isDirectory() == false || inputFile.canRead() == false) {
			printCliHelp("ERROR: unable to access the specified directory");
		}
		
		BatchImport batchImportTask = new BatchImport(config, tableName, inputFile);
		
		//undertake the task
		try {
			batchImportTask.doTask();
		} catch (TaskException e) {
			System.err.println("ERROR during data import:" + "\n" + e.getMessage());
			System.exit(-1);
		}
		
		System.out.println("SUCCESS: a total of " + batchImportTask.getTatalCount() + " records have been created");
		System.exit(0);
	}

	/*
	 * undertake the update origin task
	 */
	// we need to use a table so get the name of the table
	private static void doUpdateOriginTask() {
		
		// we need to use a table so get the name of the table
		String tableName = cmd.getOptionValue("table");
	
		if(StringUtils.isEmpty(tableName) == true) {
			printCliHelp("ERROR: the table name is required");
		}
		
		// input file
		String inputPath = cmd.getOptionValue("dataset");
		
		if(StringUtils.isEmpty(inputPath) == true) {
			printCliHelp("ERROR: the path to the rhizome database is required");
		}
		
		File inputFile = new File(inputPath);
		
		if(inputFile.isDirectory() == false || inputFile.canRead() == false) {
			printCliHelp("ERROR: unable to access the specified directory");
		}
		
		UpdateOrigin updateOriginTask = new UpdateOrigin(config, tableName, inputFile);
		
		//undertake the task
		try {
			updateOriginTask.doTask();
		} catch (TaskException e) {
			System.err.println("ERROR: during data import:" + "\n" + e.getMessage());
			System.exit(-1);
		} finally {
			try {
				updateOriginTask.closeConnection();
			} catch (SQLException e) {
				System.err.println("ERROR: during database connection close");
				System.exit(-1);
			}
		}

		System.out.println("SUCCESS: " + updateOriginTask.getUpdateCount() + " records updated successfully");
		System.exit(0);
	}
	
	/*
	 * undertake the statistics task
	 */
	private static void doStatisticsTask() {
		
		// we need to use a table so get the name of the table
		String tableName = cmd.getOptionValue("table");
	
		if(StringUtils.isEmpty(tableName) == true) {
			printCliHelp("ERROR: the table name is required");
		}
		
		StatisticalAnalysis statisticalAnalysisTask = new StatisticalAnalysis(config, tableName); 
		
		// undertake the task
		try {
			statisticalAnalysisTask.doTask();
		} catch (TaskException e) {
			System.err.println("ERROR: unable to complete statistical analysis:" + "\n" + e.getMessage());
			System.exit(-1);
		} finally {
			try {
				statisticalAnalysisTask.closeConnection();
			} catch (SQLException e) {
				System.err.println("ERROR: during database connection close");
				System.exit(-1);
			}
		}
		
		System.exit(0);	
	}
	
//	/*
//	 * undertake the basic graph task
//	 * 
//	 * deprecated until such time as sender information is stored in Rhizome
//	 */
//	private static void doBasicGraphTask() {
//		
//		// we need to use a table so get the name of the table
//		String tableName = cmd.getOptionValue("table");
//	
//		if(StringUtils.isEmpty(tableName) == true) {
//			printCliHelp("ERROR: the table name is required");
//		}
//		
//		// output file
//		String outputPath = cmd.getOptionValue("output");
//		
//		if(StringUtils.isEmpty(outputPath) == true) {
//			printCliHelp("ERROR: the path to the rhizome database is required");
//		}
//		
//		File outputFile = new File(outputPath);
//		
//		if(outputFile.exists() == true) {
//			printCliHelp("ERROR: the specified output file already exists");
//		}
//		
//		// requested sync window
//		String syncWindow = cmd.getOptionValue("window");
//		
//		Long syncWindowAsLong = null;
//		
//		if(StringUtils.isEmpty(syncWindow) == false) {
//			
//			try {
//				syncWindowAsLong = Long.parseLong(syncWindow);
//			} catch(NumberFormatException e) {
//				printCliHelp("ERROR: unable to parse the requested syncWindow");
//			}
//		}
//		
//		BasicGraph basicGraphTask = new BasicGraph(config, tableName, outputFile, syncWindowAsLong); 
//		
//		// undertake the task
//		try {
//			basicGraphTask.doTask();
//		} catch (TaskException e) {
//			System.err.println("ERROR: unable to complete graph construction:" + "\n" + e.getMessage());
//			System.exit(-1);
//		} finally {
//			try {
//				basicGraphTask.closeConnection();
//			} catch (SQLException e) {
//				System.err.println("ERROR: during database connection close");
//				System.exit(-1);
//			}
//		}
//		
//		System.out.println("SUCCESS: the specified output file has been created");
//		
//		System.exit(0);	
//	}
	
	/*
	 * undertake the bundles of time chart
	 */
	private static void doBundlesOverTimeTask() {
		
		// we need to use a table so get the name of the table
		String tableName = cmd.getOptionValue("table");
	
		if(StringUtils.isEmpty(tableName) == true) {
			printCliHelp("ERROR: the table name is required");
		}
		
		// output file
		String outputPath = cmd.getOptionValue("output");
		
		if(StringUtils.isEmpty(outputPath) == true) {
			printCliHelp("ERROR: the path to the rhizome database is required");
		}
		
		File outputFile = new File(outputPath);
		
		if(outputFile.exists() == true) {
			printCliHelp("ERROR: the specified output file already exists");
		}
		
		BundlesOverTime bundlesOverTimeTask = new BundlesOverTime(config, tableName, outputFile);
		
		// undertake the task
		try {
			bundlesOverTimeTask.doTask();
		} catch (TaskException e) {
			System.err.println("ERROR: unable to complete file output creation:" + "\n" + e.getMessage());
			System.exit(-1);
		} finally {
			try {
				bundlesOverTimeTask.closeConnection();
			} catch (SQLException e) {
				System.err.println("ERROR: during database connection close");
				System.exit(-1);
			}
		}
		
		System.out.println("SUCCESS: the specified output file has been created");
		
		System.exit(0);	
	}
	
	
	/*
	 * output the application options
	 */
	private static void printCliHelp(String message) {
		System.out.println(message);
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -jar RhizomeAnalysis.jar", createOptions());
		System.exit(-1);
	}

	/*
	 * create the command line options used by the app
	 */
	private static Options createOptions() {

		Options options = new Options();

		// task type
		OptionBuilder.withArgName("string");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("task to undertake");
		OptionBuilder.isRequired(true);
		options.addOption(OptionBuilder.create("task"));

		// properties file path
		OptionBuilder.withArgName("path");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("path to the properties file");
		OptionBuilder.isRequired(true);
		options.addOption(OptionBuilder.create("properties"));

		// table name
		OptionBuilder.withArgName("string");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("name of table to work with");
		OptionBuilder.isRequired(true);
		options.addOption(OptionBuilder.create("table"));

		// path to input database
		OptionBuilder.withArgName("path");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("path to a single input rhizome database");
		options.addOption(OptionBuilder.create("input"));
		
		// id of the tablet
		OptionBuilder.withArgName("string");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("id of the tablet");
		options.addOption(OptionBuilder.create("tablet"));
		
		// parent directory of data to import
		OptionBuilder.withArgName("path");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("path to the parent directory of a dataset");
		options.addOption(OptionBuilder.create("dataset"));
		
		// path to output file
		OptionBuilder.withArgName("path");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("path to an output file");
		options.addOption(OptionBuilder.create("output"));
		
		return options;
	}
}
