/**
 * 
 */
package com.github.nicosensei.batch.elasticsearch;

import org.apache.log4j.Level;

import com.github.nicosensei.batch.BatchException;

/**
 * @author nicolas
 *
 */
public class MappingCreationFailedException extends BatchException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1095268691743122833L;

	public MappingCreationFailedException(final String indexName, final String docType) {
		super(
			"CREATE_MAPPING_FAILED", 
			"Failed to create mapping {0} in index {1}", 
			new String[] { docType, indexName }, 
			Level.FATAL);
	}
	
	public MappingCreationFailedException(
			final String indexName, final String docType, final Throwable cause) {
		super(
			"CREATE_MAPPING_FAILED", 
			"Failed to create mapping {0} in index {1}", 
			new String[] { docType, indexName }, 
			Level.FATAL,
			cause);
	}

}
