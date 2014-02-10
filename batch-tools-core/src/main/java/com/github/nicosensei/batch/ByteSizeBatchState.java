/**
 *
 */
package com.github.nicosensei.batch;

import java.io.File;
import java.io.UnsupportedEncodingException;

import com.github.nicosensei.batch.input.InputFileReader;
import com.github.nicosensei.batch.input.InputLine;
import com.github.nicosensei.commons.exceptions.Unexpected;
import com.github.nicosensei.commons.utils.datatype.ByteCountFormatter;

/**
 * @author ngiraud
 *
 */
public class ByteSizeBatchState extends AbstractBatchState {

    private final static String fileEncoding = BatchExecutor.getInstance().getProperty(
            InputFileReader.class, "encoding");
    
    private final static int EOL_BYTES;
    
    private long processedLines = 0L;
    
    static {
    	try {
			EOL_BYTES = "\n".getBytes(fileEncoding).length;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
    }

    public ByteSizeBatchState(final String inputFilePath) {
        super(new File(inputFilePath).length());
    }

    @Override
    public void notifyLineProcessed(InputLine line) {
        try {
            // Also add the newline size (not brought back by the reader)
            incrementUnitsProcessed(line.getLine().getBytes(fileEncoding).length + EOL_BYTES);
            processedLines++;
        } catch (UnsupportedEncodingException e) {
            throw new Unexpected(e);
        }
    }

    /**
	 * @return the processedLines
	 */
	public long getProcessedLines() {
		return processedLines;
	}

	@Override
    public void logStatus() {
        BatchExecutor.getInstance().logInfo(
                ByteCountFormatter.humanReadableByteCount(getUnitsProcessed())
                + "/"
                + ByteCountFormatter.humanReadableByteCount(getUnitsToProcess())
        + " processed ("
        + PERCENTAGE.format(getCompletionPercentage()) + "%).");
    }



}
