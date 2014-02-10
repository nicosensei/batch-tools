/**
 * 
 */
package com.github.nicosensei.batch.input;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import com.github.nicosensei.batch.BatchException;
import com.github.nicosensei.batch.BatchTestCase;
import com.github.nicosensei.batch.input.BigFileReader;
import com.github.nicosensei.batch.input.InputFileException;
import com.github.nicosensei.batch.input.InputFileSection;

/**
 * @author ngiraud
 *
 */
public class BigFileReaderTest extends BatchTestCase {

	private static class Padding {
		final String filler;
		final int lineLenght;
		private Padding(String filler, int linelenght) {
			super();
			this.filler = filler;
			this.lineLenght = linelenght;
		}
	}

	private static class TestReader extends BigFileReader<TestInputLine> {

		public TestReader(
				String inputFile, 
				int sectionSize,
				boolean ignoreEmptyLines) throws InputFileException {
			super(inputFile, sectionSize, ignoreEmptyLines);
		}

		@Override
		protected TestInputLine parseLine(String line) throws InputFileException {
			return new TestInputLine(line, "\\s+");
		}

	}

	public final void testReadLineByLine() throws IOException, BatchException {
		int[] lineCounts = new int[] { 10, 100, 1000, 10000, 100000 };
		Padding pad = new Padding("*", 10);
		for (int lc : lineCounts) {
			File testFile = genererateTestFile(lc, pad);
			System.out.println("Test file " + testFile.getAbsolutePath() + " (" + lc + " lines)");

			try {
				TestReader bfr = new TestReader(testFile.getAbsolutePath(), 5, true);
				long readCount = 0;
				while (bfr.readLine() != null) {
					readCount++;
				}
				assertEquals(lc, readCount);
			} finally {
				if (!testFile.delete()) {
					testFile.deleteOnExit();
				}
			}
		}

	}

	public final void testReadBySection() throws IOException, BatchException {
		int[] lineCounts = new int[] { 10, 1000, 10000, 100000, 1000000 };
		Padding pad = new Padding("*", 10);
		for (int lc : lineCounts) {
			File testFile = genererateTestFile(lc, pad);
			System.out.println("Test file " + testFile.getAbsolutePath() + " (" + lc + " lines)");

			try {
				TestReader bfr = new TestReader(testFile.getAbsolutePath(), 1000, true);
				long readCount = 0;
				while (true) {
					InputFileSection<TestInputLine> section = bfr.readSection();
					long linesRead = section.getLines().size();
					boolean lastSection = section.noMoreInput();
					if (!lastSection) {
						assertEquals(1000, linesRead);
					} else {				
						assertTrue(linesRead <= 1000);
					}
					readCount += linesRead;

					if (lastSection) {
						break;
					}
				}
				assertEquals(lc, readCount);
			} finally {

				if (!testFile.delete()) {
					testFile.deleteOnExit();
				}
			}
		}

	}


	private File genererateTestFile(long lineCount, Padding p) throws IOException {
		File f = File.createTempFile(
				BigFileReaderTest.class.getSimpleName(), 
				"" + System.currentTimeMillis());
		PrintWriter pw = new PrintWriter(f);
		for (long l = 1; l <= lineCount; l++) {
			String line = Long.toString(l);
			if (p != null) {
				while (line.length() < p.lineLenght) {
					line += p.filler;
				}
			}
			pw.println(line);
		}
		pw.close();
		return f;
	}

}
