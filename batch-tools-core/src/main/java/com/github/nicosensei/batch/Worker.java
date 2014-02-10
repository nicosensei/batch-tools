/**
 *
 */
package com.github.nicosensei.batch;

import java.util.List;

import org.apache.log4j.Level;

import com.github.nicosensei.batch.input.InputFileException;
import com.github.nicosensei.batch.input.InputFileReader;
import com.github.nicosensei.batch.input.InputFileSection;
import com.github.nicosensei.batch.input.InputLine;


/**
 * @author ngiraud
 *
 */
public abstract class Worker<L extends InputLine> extends Thread {


    private InputFileReader<L> input;
    private BatchState state;

    protected BatchExecutor executor;

    private boolean alive = true;

    protected Worker(InputFileReader<L> input, BatchState state) {
        this.input = input;
        this.state = state;
        this.executor = BatchExecutor.getInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        while (alive) {

            // Read a section from the input file
            InputFileSection<L> section = null;
            try {
                section = input.readSection();
            } catch (InputFileException e) {
            	handleBatchException(e);
            	continue;
            }

            try {
                // Process it
                for (L line : preProcessSection(section)) {
                    processLine(line);
                    state.notifyLineProcessed(line);
                }

                sectionComplete();
            } catch (BatchException e) {
                handleBatchException(e);
            }

            // Stop if there's no more input available
            if (section.noMoreInput()) {
                break;
            }
        }
        if (alive) {
            try {
                jobComplete();
            } catch (BatchException e) {
                handleBatchException(e);
            }
        }
    }

    public BatchState getBatchState() {
        return state;
    }

    protected abstract void processLine(L line) throws BatchException;
    
    /**
     * By default simply return all the lines in the section. Sub-classes can override this 
     * method to perform specific processing (aggregation, filtering).
     * @param section
     * @return
     * @throws BatchException
     */
    protected List<L> preProcessSection(InputFileSection<L> section) throws BatchException {
    	return section.getLines();
    }
    
    protected abstract void sectionComplete() throws BatchException;
    protected abstract void jobComplete() throws BatchException;

    protected void handleBatchException(final BatchException e) {
    	BatchExecutor executor = BatchExecutor.getInstance();
    	executor.logError(e);
        state.notifyError(e);
        if (Level.FATAL.equals(e.getCriticity())) {
            executor.logInfo(
                    "Fatal error "
                    + e.getClass().getSimpleName());
            this.alive = false;
        }
    }

}
