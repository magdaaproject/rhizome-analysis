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

public class TaskException extends Exception {

	private static final long serialVersionUID = 2832895797887833777L;

	/**
	 * Constructs a new exception with null as its detail message.
	 */
	public TaskException() {
		super();
	}

	/**
	 * Constructs a new exception with the specified detail message.
	 * @param message  the detail message.
	 */
	public TaskException(String message) {
		super(message);
	}

	/**
	 * Constructs a new exception with the specified cause
	 * @param cause the cause
	 */
	public TaskException(Throwable cause) {
		super(cause);
	}

	/**
	 * Constructs a new exception with the specified detail message and cause.
	 * 
	 * @param message the detail message 
	 * @param cause the cause
	 */
	public TaskException(String message, Throwable cause) {
		super(message, cause);
	}

}
