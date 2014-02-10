/**
 *
 */
package com.github.nicosensei.batch.input;

/**
 * Base class for a line in a input file. A line is constituted of a number
 * of fields separated by a given separator string.
 *
 * @author ngiraud
 *
 */
public interface InputLine {

    /**
     * @return the line as is.
     */
    String getLine();

    /**
     * @return the separator string
     */
    String getSeparator();

    /**
     * @return the fields
     */
    String[] getFields();

}
