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

/**
 * Abstract task representing tasks undertaken by this application
 */
public abstract class AbstractTask { 
	
	public abstract void doTask() throws TaskException;

}