/**
 *
 */
package com.github.nicosensei.batch;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;

import com.github.nicosensei.batch.input.AbstractInputFileReader;
import com.github.nicosensei.batch.input.InputLine;



/**
 * @author ngiraud
 *
 */
public abstract class ResultFileBatchWorker<L extends InputLine> extends Worker<L> {

    private List<String> outputBuffer = new LinkedList<String>();

    /**
     * @param input
     * @param state
     */
    protected ResultFileBatchWorker(
            AbstractInputFileReader<L> input, BatchState state) {
        super(input, state);
    }

    @Override
    protected void sectionComplete() throws BatchException {

        if (outputBuffer.isEmpty()) {
            return;
        }

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        for (String line : outputBuffer) {
            pw.println(line);
        }

        ((ResultFileBatchState) getBatchState()).logResult(sw.toString());
        pw.close();
        outputBuffer.clear();
    }

    protected void addResult(String res) {
        outputBuffer.add(res);
    }

    @Override
    protected abstract void processLine(L line) throws BatchException;

}
