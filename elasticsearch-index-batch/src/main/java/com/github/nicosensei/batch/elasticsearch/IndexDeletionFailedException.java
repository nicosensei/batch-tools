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
public class IndexDeletionFailedException extends BatchException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1095268691743122833L;

	public IndexDeletionFailedException(final String indexName) {
		super(
			"DELETE_INDEX_FAILED", 
			"Failed to delete index {0}", 
			new String[] { indexName }, 
			Level.FATAL);
	}

}
