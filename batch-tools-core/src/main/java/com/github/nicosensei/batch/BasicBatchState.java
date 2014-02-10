/**
 *
 */
package com.github.nicosensei.batch;

import java.text.DecimalFormat;

import com.github.nicosensei.batch.input.InputLine;


/**
 * @author ngiraud
 *
 */
public abstract class BasicBatchState extends AbstractBatchState {

    protected static final DecimalFormat PERCENTAGE =
        new DecimalFormat("###.##");

    protected BasicBatchState(long linesToProcess) {
        super(linesToProcess);
    }

    public synchronized void notifyLineProcessed(InputLine l) {
        incrementUnitsProcessed(1);
    }

    @Override
    public void logStatus() {
        BatchExecutor.getInstance().logInfo(getUnitsProcessed() + "/" + getUnitsToProcess()
        + " lines processed ("
        + PERCENTAGE.format(getCompletionPercentage()) + "%).");

    }

}
