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
    	StringBuilder sb = new StringBuilder()
    		.append(Long.toString(getUnitsProcessed()))
    		.append("/")
    		.append(Long.toString(getUnitsToProcess()))
    		.append(" lines processed (")
    		.append(PERCENTAGE.format(getCompletionPercentage()))
    		.append("%).");
        BatchExecutor.getInstance().logInfo(sb.toString());

    }

}
