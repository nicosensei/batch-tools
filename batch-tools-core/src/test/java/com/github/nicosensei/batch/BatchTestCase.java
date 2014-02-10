/**
 *
 */
package com.github.nicosensei.batch;

import java.net.URL;

import com.github.nicosensei.batch.BatchExecutor;

import junit.framework.TestCase;

/**
 * @author ngiraud
 *
 */
public abstract class BatchTestCase extends TestCase {

    private final long initTime = System.currentTimeMillis();

    @Override
    protected void setUp() throws Exception {
    	URL url = getClass().getResource("/settings.xml");
    	BatchExecutor.init(url.getPath(), getName());
    }

    @Override
    protected void tearDown() throws Exception {
        BatchExecutor.cleanup(null, initTime);
    }

}