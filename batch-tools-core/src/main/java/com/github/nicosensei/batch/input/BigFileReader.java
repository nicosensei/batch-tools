/**
 * 
 */
package com.github.nicosensei.batch.input;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.input.CountingInputStream;

import com.github.nicosensei.batch.BatchExecutor;
import com.github.nicosensei.commons.utils.datatype.ByteCountFormatter;

/**
 * 
 * This reader is intended for huge input files (several gigas or dozen of gigas) where the batch will run 
 * for a long time and we cannot guarantee that the file handler will stay valid (NFS mounts for instance).
 * 
 * @author ngiraud
 *
 */
public abstract class BigFileReader<L extends InputLine> implements InputFileReader<L> {

    private static final String DEFAULT_ENCODING = BatchExecutor.getInstance().getProperty(
            InputFileReader.class, "encoding");
    
    /**
     * The number of retries after an {@link IOException} when reading.
     */
    private int READ_RETRIES = BatchExecutor.getInstance().getIntProperty(
    		BigFileReader.class, "readRetries", 1);
    
    /**
     * The tilme in milliseconds to wait between read retires.
     */
    private int RETRY_DELAY_MS = 1000 * BatchExecutor.getInstance().getIntProperty(
    		BigFileReader.class, "readRetryDelaySeconds", 1);

    /**
     * Number of lines per section.
     */
    private int sectionSize = -1;

    private boolean ignoreEmptyLines = true;
    
    /**
     * The underlying input stream
     */
    private CountingInputStream inStream;
    
    /**
     * The offset we are at in the input stream.
     */
    private long offset;
    
    /**
     * Buffered reader for the input file.
     */
    private BufferedReader bufReader;

    private final String inputFilePath;
    
    private final String inputFileEncoding;
    
    public BigFileReader(
            String inputFile,
            String inputFileEncoding,
            int sectionSize,
            boolean ignoreEmptyLines) throws InputFileException {

    	this.inputFilePath = inputFile;
    	this.inputFileEncoding = inputFileEncoding;
        this.ignoreEmptyLines = ignoreEmptyLines;
        this.sectionSize = sectionSize;
        resetInput();
        BatchExecutor.getInstance().logInfo("Input encoding set to " + inputFileEncoding);
        BatchExecutor.getInstance().logInfo("Processing input file by chunks of "
                + sectionSize + " lines.");
    }
    
    public BigFileReader(
            String inputFile,
            int sectionSize,
            boolean ignoreEmptyLines) throws InputFileException {
    	this(inputFile, DEFAULT_ENCODING, sectionSize, ignoreEmptyLines);
    }
    
    /**
     * Closes the reader.
     * @throws InputFileException
     */
    public synchronized void close() throws InputFileException {
        try {
            bufReader.close();
        } catch (IOException e) {
            throw InputFileException.closeFailed(inputFilePath, e);
        }
    }

    /**
     * Atomically obtain a section of the combined path file
     * @return
     * @throws InputFileException
     */
    public synchronized InputFileSection<L> readSection()
    throws InputFileException {

        List<L> lines = new LinkedList<L>();

        while (lines.size() < sectionSize) {
            String l = readOneLine();
            if (l == null) {
                break;
            }
            if (ignoreEmptyLines && lineIsEmpty(l)) {
                continue; // skip empty lines
            }
            lines.add(parseLine(l));
        }

        return new InputFileSection<L>(lines, lines.size() < sectionSize);

    }

    /**
     * Atomically obtain a line of the combined path file
     * @return
     * @throws InputFileException
     */
    public synchronized L readLine() throws InputFileException {
    	L line = null;
        while (line == null) {

            String l = readOneLine();
            if (l == null) {
                break;
            }
            if (ignoreEmptyLines && lineIsEmpty(l)) {
                continue; // skip empty lines
            }
            line = parseLine(l);
        }
        return line;

    }

    @Override
    public String getEncoding() {
        return inputFileEncoding;
    }

    @Override
	public int getSectionSize() {
		return sectionSize;
	}

	protected abstract L parseLine(String line) throws InputFileException;

    protected String getInputFilePath() {
        return inputFilePath;
    }

    private boolean lineIsEmpty(String l) {
        return l.trim().isEmpty();
    }
    
    protected String readOneLine() throws InputFileException {
    	
    	BatchExecutor executor = BatchExecutor.getInstance();
    	
    	int tryCount = 1;
    	this.offset = inStream.getByteCount();
    	while (true) {
    		try {
    			return bufReader.readLine();
        	} catch (final IOException ioe) {
        		if (tryCount > READ_RETRIES) {
        			executor.logInfo("Failed reading from " + inputFilePath + " after " + tryCount + " tries");
        			throw InputFileException.readError(inputFilePath, ioe);
        		}
        		tryCount++;
        		executor.logInfo("Will retry reading from " 
        				+ inputFilePath + " ("+ tryCount + " tries out of " + READ_RETRIES);
        		try {
        			Thread.sleep(RETRY_DELAY_MS);
        		} catch (InterruptedException e) {
        			executor.logError(e);
        		}
        		resetInput();
        	}
    	}
    }
    
    private void resetInput() throws InputFileException {
    	try {
    		if (this.bufReader != null) {
    			this.bufReader.close(); // should close inner stream
    		}
    		
    		this.inStream = new CountingInputStream(new FileInputStream(inputFilePath));
    		if (this.offset > 0) {
    			this.inStream.skip(this.offset); // skip to latest offset
    			BatchExecutor.getInstance().logInfo("Skipped " 
    					+ ByteCountFormatter.humanReadableByteCount(this.offset) 
    					+ " from input file " + inputFilePath);
    		}
            this.bufReader = new BufferedReader(
                    new InputStreamReader(this.inStream, getEncoding()));
    	} catch (final FileNotFoundException e) {
            throw InputFileException.fileNotFound(inputFilePath);
        } catch (final UnsupportedEncodingException e) {
            throw InputFileException.ioError(inputFilePath, e);
        } catch (final IOException ioe) {
    		throw InputFileException.readError(inputFilePath, ioe);
    	}    	
    }

}
