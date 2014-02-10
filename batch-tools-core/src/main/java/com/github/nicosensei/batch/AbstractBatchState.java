/**
 *
 */
package com.github.nicosensei.batch;

import java.text.DecimalFormat;
import java.util.LinkedList;

import com.github.nicosensei.batch.input.InputLine;


/**
 * @author ngiraud
 *
 */
public abstract class AbstractBatchState implements BatchState {

    private final Long unitsToProcess;
    private Long unitsProcessed;

    private LinkedList<BatchException> errors = new LinkedList<BatchException>();

    AbstractBatchState(final long unitsToProcess) {
        super();
        this.unitsToProcess = unitsToProcess;
        this.unitsProcessed = 0L;
    }

    AbstractBatchState(
            final long unitsToProcess,
            long unitsProcessed) {
        super();
        this.unitsToProcess = unitsToProcess;
        this.unitsProcessed = unitsProcessed;
    }

    protected static final DecimalFormat PERCENTAGE =
        new DecimalFormat("###.##");

    @Override
    public double getCompletionPercentage() {
        return (100 * unitsProcessed.doubleValue())
                / unitsToProcess.doubleValue();
    }

    @Override
    public abstract void notifyLineProcessed(InputLine l);

    @Override
    public abstract void logStatus();

    @Override
    public BatchException[] getErrors() {
        return errors.toArray(new BatchException[errors.size()]);
    }

    @Override
    public synchronized void notifyError(BatchException e) {
        errors.add(e);
    }

    protected final Long getUnitsToProcess() {
        return unitsToProcess;
    }

    protected final Long getUnitsProcessed() {
        return unitsProcessed;
    }

    final void incrementUnitsProcessed(long value) {
        unitsProcessed += value;
    }

}
