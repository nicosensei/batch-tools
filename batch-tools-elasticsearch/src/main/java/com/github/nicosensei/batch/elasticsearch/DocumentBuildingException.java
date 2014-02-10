/**
 * 
 */
package com.github.nicosensei.batch.elasticsearch;

import org.apache.log4j.Level;

import com.github.nicosensei.batch.BatchException;

/**
 * @author S818203
 *
 */
public class DocumentBuildingException extends BatchException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9081307746016654030L;
	public static final String CODE = "DOCUMENT_BUILD_ERROR";
	
	public DocumentBuildingException(final String inputLine) {
		super(CODE, 
				"Error when building document from line \"{0}\"", 
				new String[] { inputLine }, 
				Level.ERROR);
	}
	
	public DocumentBuildingException(final String inputLine, final Throwable cause) {
		super(CODE, 
				"Error when building document from line \"{0}\"", 
				new String[] { inputLine }, 
				Level.ERROR,
				cause);
	}

}
