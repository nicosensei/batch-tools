/**
 * 
 */
package com.github.nicosensei.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * Offers a method that initializes a {@link BatchExecutor} from 
 * a classpath resource. Intended for unit tests.
 * 
 * @author nicolas
 *
 */
public final class BatchExecutorUnitTestSupport {
	
	/**
	 * Reloads the {@link BatchExecutor} configuration, finalizing the executor first if it
	 * has previously been initialized.
	 * @param resourcePath the classpath resource path for the settings XML file
	 * @param initTimeInMillis the executor initialization time, will be muted
	 * @throws IOException if the I/O went wrong.
	 */
	public final static void reloadSettings(
			final String resourcePath,
			long initTimeInMillis) throws IOException {
		
		BatchExecutor exec = BatchExecutor.getInstance();
		if (exec != null) {
			exec.logInfo("Finalizing executor.");
			BatchExecutor.cleanup(null, initTimeInMillis);
			initTimeInMillis = System.currentTimeMillis();
		}
		
		File tmpSettings = File.createTempFile("tmpSettings", ".xml");
		PrintWriter pw = new PrintWriter(tmpSettings);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(
				BatchExecutorUnitTestSupport.class.getResourceAsStream(resourcePath)));
		String line = null;
		while ((line = in.readLine()) != null) {
			pw.println(line);
		}
		
		pw.close();
		
		BatchExecutor.init(
				tmpSettings.getAbsolutePath(), 
				BatchExecutorUnitTestSupport.class.getSimpleName());
	}

}
