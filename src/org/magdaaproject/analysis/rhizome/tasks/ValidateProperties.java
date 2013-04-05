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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConversionException;

/**
 * class representing a task to validate the properties file
 */
public class ValidateProperties extends AbstractTask {
	
	/*
	 * private class level constants
	 */
	private final String[] requiredProperties = {
			"db.host",
			"db.database",
			"db.user",
			"db.password"};
	
	/*
	 * private class level variables
	 */
	private Configuration config;
	
	/**
	 * construct a new class to validate the properties file
	 * @param config the Configuration object representing the properties contained in the file
	 */
	public ValidateProperties(Configuration config) {
		
		// validate the parameters
		if(config == null) {
			throw new IllegalArgumentException("config is a required parameter");
		}
		
		this.config = config;
	}
	
	
	/**
	 * undertake the task of validating the properties file
	 * 
	 * @throws TaskException if something bad happens
	 */
	public void doTask() throws TaskException {
		
		String tmp = null;
		
		// convert the array of properties into a list
		List<String> properties = Arrays.asList(requiredProperties);
		
		// loop over the list of properties to ensure each one exists
		for (String property: properties) {
			try {
				tmp = config.getString(property, null);
				
				if(tmp == null) {
					throw new TaskException("The required property '" + property + "' was not found in the properties file");
				}
			}
			catch (ConversionException e) {
				throw new TaskException("Unable to read the value for property '" + property + "'");
			}
		}
		
	}

}
