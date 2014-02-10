/**
 *
 */
package com.github.nicosensei.batch.input;

import com.github.nicosensei.batch.input.BasicInputLine;

import junit.framework.TestCase;

/**
 * @author ngiraud
 *
 */
public class BasicInputLineTest extends TestCase {

    public static final void testSeparatorDisplay() {

        BasicInputLine l = new TestInputLine("blah blah", " ");
        assertEquals(" ", l.getSeparator());
        assertEquals("blah blah", l.getLine());

        l = new TestInputLine("blah     blah", "\\s+");
        assertEquals(" ", l.getSeparator());
        assertEquals("blah blah", l.getLine());

        l = new TestInputLine("blah\tblah", "\t");
        assertEquals(" ", l.getSeparator());
        assertEquals("blah blah", l.getLine());
    }

}
