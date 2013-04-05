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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.filefilter.IOFileFilter;

/**
 * an instance of the Directory Walker class to get
 * details of Rhizome databases and survey xml files
 */
public class DatasetDirectoryWalker extends DirectoryWalker<Object> {
	
	public DatasetDirectoryWalker(IOFileFilter dirFilter, IOFileFilter fileFilter) {
		super(dirFilter, fileFilter, -1);
	}
	
	/**
	 * return the list of files
	 * @param startDirectory the directory start the walk
	 * @return the list of files
	 * @throws IOException if something bad happens
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ArrayList<File> getFileList(File startDirectory) throws IOException {
		List results = new ArrayList();
		walk(startDirectory, results);
		return (ArrayList<File>) results;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void handleFile(File file, int depth, Collection results) {
		// add the file the list of files
		results.add(file);
	}
}
