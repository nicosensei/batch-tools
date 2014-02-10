/**
 *
 */
package com.github.nicosensei.batch;

import java.io.IOException;

import com.github.nicosensei.commons.utils.FileUtils;



/**
 * @author ngiraud
 *
 */
public abstract class ResultFileBatchState extends BasicBatchState {

    private ResultLogger logger;

    /**
     * @param linesToProcess
     * @throws JobStateException
     */
    protected ResultFileBatchState(
            int linesToProcess,
            String logId,
            String logPattern)
    throws BatchStateException {

        super(linesToProcess);

        logger = new ResultLogger(
                logId,
                getResultsFilePath(logId),
                logPattern);
    }

    public synchronized void logResult(String resultMessage) {
        logger.logResult(resultMessage);
    }

    public void copyResultsTo(String outputFile) throws BatchStateException {
        try {
            FileUtils.copyFile(getResultFilePath(), outputFile);
        } catch (final IOException ioe) {
            throw BatchStateException.copyFailed(ioe);
        }
        BatchExecutor.getInstance().logInfo("Copied result file to " + outputFile);
    }

    protected String getResultFilePath() {
        return logger.getResultFilePath();
    }

    /**
     * By default return the given logId parameter.
     * Override if another behavior is desired.
     * @param logId the log ID
     * @return the result file base name
     */
    protected String getResultsFilePath(String logId) {
        return logId;
    }

}
