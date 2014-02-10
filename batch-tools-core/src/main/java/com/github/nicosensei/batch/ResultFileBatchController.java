/**
 *
 */
package com.github.nicosensei.batch;

import java.io.File;

import com.github.nicosensei.batch.input.InputLine;


/**
 * @author ngiraud
 *
 */
public abstract class ResultFileBatchController<
L extends InputLine,
J extends ResultFileBatchWorker<L>>
extends Batch<L, J> {

    @Override
    public ResultFileBatchState getBatchState() {
        return (ResultFileBatchState) super.getBatchState();
    }

    @Override
    protected void onComplete() throws BatchException {
        File resultFile = new File(getBatchState().getResultFilePath());
        if (resultFile.exists() && resultFile.length() == 0) {
            BatchExecutor.getInstance().registerFileForCleanup(resultFile);
        }

    }



}
