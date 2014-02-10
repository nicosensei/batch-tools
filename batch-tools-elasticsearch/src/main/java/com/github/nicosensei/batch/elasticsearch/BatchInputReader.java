package com.github.nicosensei.batch.elasticsearch;

import java.util.ArrayList;
import java.util.EventListener;
import java.util.List;

import com.github.nicosensei.batch.BatchExecutor;
import com.github.nicosensei.batch.input.BigFileReader;
import com.github.nicosensei.batch.input.InputFileException;
import com.github.nicosensei.batch.input.InputLine;
import com.github.nicosensei.commons.utils.datatype.TimeFormatter;

public abstract class BatchInputReader<D extends InputLine> extends BigFileReader<D> {
	
	public interface CooldownListener extends EventListener {
		void handleCooldownEvent(boolean cooldown);
	}

	/**
	 * Indicates that a cooldown time should be respected after having read 
	 * this number of lines. If set to 0 or less, disables cooldown.
	 */
	private final long cooldownAfterLinesRead;

	/**
	 * Duration of the cooldown time in milliseconds.Any call to {@link #readSection()} or 
	 * {@link #readLine()} during cooldown will put the current thread to sleep for
	 * this amount of time (unless interrupted).
	 */
	private final int cooldownTimeInMillis;

	private long linesReadSinceCooldown = 0L;
	
	/**
	 * Listeners.
	 */
	private List<CooldownListener> cooldownListeners = new ArrayList<CooldownListener>();

	public BatchInputReader(
			String inputFile, 
			String inputFileEncoding, 
			int sectionSize,
			boolean ignoreEmptyLines) throws InputFileException {
		super(inputFile, inputFileEncoding, sectionSize, ignoreEmptyLines);
		this.cooldownAfterLinesRead = 0L;
		this.cooldownTimeInMillis = 0;
	}
	
	
	public BatchInputReader(
			String inputFile,
			String inputFileEncoding,
			int sectionSize,
			boolean ignoreEmptyLines,
			long cooldownAfterLinesRead,
			int cooldownTimeInMillis) throws InputFileException {
		super(inputFile, inputFileEncoding, sectionSize, ignoreEmptyLines);
		this.cooldownAfterLinesRead = cooldownAfterLinesRead;
		this.cooldownTimeInMillis = cooldownTimeInMillis;
	}
	
	public void addCooldownListener(CooldownListener l) {
		this.cooldownListeners.add(l);
	}

	@Override
	protected abstract D parseLine(String line) throws InputFileException;

	@Override
	protected String readOneLine() throws InputFileException {

		try {
			return super.readOneLine();
		} finally {
			// Check if cooldown should happen
			if (cooldownAfterLinesRead > 0) {
				linesReadSinceCooldown++;

				if (linesReadSinceCooldown >= cooldownAfterLinesRead) {
					fireCooldownEvent(true);
					BatchExecutor exec = BatchExecutor.getInstance();
					try {
						exec.logInfo("Input reader cooldown for " 
								+ TimeFormatter.formatDuration(cooldownTimeInMillis));
						Thread.sleep(cooldownTimeInMillis);
					} catch (InterruptedException e) {
						exec.logInfo(Thread.currentThread().getName() 
								+ " interrupted before colldown end!");
					}
					fireCooldownEvent(false);
					linesReadSinceCooldown = 0L;
				}
			}
		}
	}
	
	private void fireCooldownEvent(boolean cooldown) {
		for (CooldownListener l : cooldownListeners) {
			l.handleCooldownEvent(cooldown);
		}
	}

}
