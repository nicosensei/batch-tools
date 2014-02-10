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
public class SkipLimitExceededException extends BatchException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1095268691743122833L;

	public SkipLimitExceededException(final int skipLimit) {
		super(
			"OVER_SKIP_LIMIT", 
			"Exceeded skip limit of {0}", 
			new String[] { Integer.toString(skipLimit) }, 
			Level.FATAL);
	}

}
