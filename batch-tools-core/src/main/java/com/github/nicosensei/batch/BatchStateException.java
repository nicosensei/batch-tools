/**
 *
 */
package com.github.nicosensei.batch;

import org.apache.log4j.Level;



/**
 * @author ngiraud
 *
 */
public class BatchStateException extends BatchException {

    private static final long serialVersionUID = 2067714794562082336L;

    private BatchStateException(
            String code,
            String messageFormat,
            String[] params, Level criticity,
            Throwable cause) {
        super(code, messageFormat, params, criticity, cause);
    }

    public static BatchStateException openResultFileFailed(
            String filePath, Exception cause) {
        return new BatchStateException(
                "JobState_OPEN_RESULT_FILE_FAILED",
                "Failed to open result file {0}",
                new String[] { filePath },
                Level.FATAL,
                cause);
    }

    public static BatchStateException initFailed(Throwable t) {
        return new BatchStateException(
                "JobState_INIT_FAILED",
                "Failed to initialize batch state: {0}",
                new String[] { t.getLocalizedMessage() },
                Level.FATAL,
                t);
    }

    public static BatchStateException copyFailed(Throwable t) {
        return new BatchStateException(
                "JobState_COPY_FAILED",
                "Failed to copy result file: {0}",
                new String[] { t.getLocalizedMessage() },
                Level.ERROR,
                t);
    }

}
