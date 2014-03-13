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
public class IndexCreationFailedException extends BatchException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1095268691743122833L;

	public IndexCreationFailedException(final String indexName) {
		super(
			"CREATE_INDEX_FAILED", 
			"Failed to create index {0}", 
			new String[] { indexName }, 
			Level.FATAL);
	}
	
	public IndexCreationFailedException(final String indexName, final Throwable cause) {
		super(
			"CREATE_INDEX_FAILED", 
			"Failed to create index {0}", 
			new String[] { indexName }, 
			Level.FATAL,
			cause);
	}

}
