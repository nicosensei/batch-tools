/**
 *
 */
package com.github.nicosensei.batch;

import com.github.nicosensei.batch.input.InputLine;

/**
 * @author ngiraud
 *
 */
public interface BatchState {

    void notifyLineProcessed(InputLine line);

    double getCompletionPercentage();

    void logStatus();

    void notifyError(BatchException e);
    
    BatchException[] getErrors();

}
