package com.github.nicosensei.batch.elasticsearch;

import com.github.nicosensei.batch.BatchExecutor;
import com.github.nicosensei.batch.ByteSizeBatchState;
import com.github.nicosensei.batch.elasticsearch.BatchInputReader.CooldownListener;
import com.github.nicosensei.batch.input.InputLine;
import com.github.nicosensei.commons.utils.datatype.ByteCountFormatter;

public class IndexBatchState extends ByteSizeBatchState
implements CooldownListener {
	
	private long linesProcessed = 0L;
	
	private boolean cooldown = false;
	
	private long linesSkipped = 0L;

	/**
	 * @param inputFilePath
	 */
	public IndexBatchState(String inputFilePath) {
		super(inputFilePath);
	}
	
	@Override
	public void handleCooldownEvent(boolean cooldown) {
		this.cooldown = cooldown;		
	}

	@Override
	public synchronized void notifyLineProcessed(InputLine line) {
		super.notifyLineProcessed(line);
		linesProcessed++;
	}
	
	public synchronized void notifyLineSkipped() {
		linesSkipped++;
	}

	public long getLinesSkipped() {
		return linesSkipped;
	}

	@Override
	public void logStatus() {
		if (!cooldown) {
			int linesFailed = getErrors().length;
			BatchExecutor.getInstance().logInfo(
					linesProcessed + " lines - "
							+ ByteCountFormatter.humanReadableByteCount(getUnitsProcessed())
							+ "/"
							+ ByteCountFormatter.humanReadableByteCount(getUnitsToProcess())
							+ " processed ("
							+ PERCENTAGE.format(getCompletionPercentage()) + "%)"
							+ (linesSkipped > 0 ? " (" + linesSkipped + " skipped)" : "")
							+ (linesFailed > 0 ? " (" + linesFailed + " failed)" : "")
							+ ".");
		}
	}

}
