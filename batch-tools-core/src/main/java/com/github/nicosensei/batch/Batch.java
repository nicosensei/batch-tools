/**
 *
 */
package com.github.nicosensei.batch;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.github.nicosensei.batch.input.InputFileException;
import com.github.nicosensei.batch.input.InputFileReader;
import com.github.nicosensei.batch.input.InputLine;



/**
 * @author ngiraud
 *
 */
public abstract class Batch<L extends InputLine, W extends Worker<L> > {

    private class StateDisplayer implements Runnable {

        private final BatchState state;

        public StateDisplayer(BatchState state) {
            this.state = state;
        }

        @Override
        public void run() {
            state.logStatus();
        }

    }

    private InputFileReader<L> inputFile;
    private BatchState state;

    private List<W> workers = new LinkedList<W>();

    public final void initialize(String[] args) throws BatchException {
        init(args);
        this.inputFile = inputFileReaderFactory();
        this.state = batchStateFactory();
    }

    public void launch() throws BatchException {

        int threadCount = getThreadCount();
        BatchExecutor exeutor = BatchExecutor.getInstance();

        for (int i = 0; i < threadCount; i++) {
            W worker = workerFactory();
            workers.add(worker);
            worker.start();
        }

        exeutor.logInfo("Started " + threadCount + " worker"
                + (threadCount > 1 ? "s." : "."));

        int controlLoopSleepTime = BatchExecutor.getInstance().getIntProperty(
                Batch.class, "sleepInSeconds", 1);

        // Launch state display thread
        int delay = BatchExecutor.getInstance().getIntProperty(
                BatchState.class, "delayInSeconds");
        ScheduledThreadPoolExecutor stateDisplay =
            new ScheduledThreadPoolExecutor(1);
        stateDisplay.scheduleAtFixedRate(
                new StateDisplayer(state),
                0, delay, TimeUnit.SECONDS);

        while (true) {
            boolean allDone = true;
            for (Worker<L> w : workers) {
                allDone &= ! w.isAlive();
            }
            if (allDone) {
                break;
            }

            try {
                Thread.sleep(controlLoopSleepTime);
            } catch (InterruptedException e) {
            }

        }

        stateDisplay.shutdown();
        this.inputFile.close();

        state.logStatus();
        onComplete();

    }

    public InputFileReader<L> getInputFile() {
        return inputFile;
    }

    protected abstract W workerFactory() throws BatchException;

    protected abstract BatchState batchStateFactory() throws BatchException;

    protected abstract InputFileReader<L> inputFileReaderFactory()
    throws InputFileException;

    protected abstract void init(String[] args) throws BatchException;

    protected abstract void onComplete() throws BatchException;

    public BatchState getBatchState() {
        return state;
    }

    protected int getThreadCount() {
        return Integer.parseInt(
                BatchExecutor.getInstance().getProperty(getClass(), "threadCount"));
    }

    protected int getSectionSize() {
        return Integer.parseInt(
                BatchExecutor.getInstance().getProperty(getClass(), "sectionSize"));
    }

}
