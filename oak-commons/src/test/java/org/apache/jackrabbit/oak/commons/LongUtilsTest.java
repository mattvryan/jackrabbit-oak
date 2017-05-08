package org.apache.jackrabbit.oak.commons;

import junit.framework.TestCase;

public class LongUtilsTest extends TestCase {
    public void testSafeAdd() {
        // Simple math
        assertEquals(0L, LongUtils.safeAdd(0L, 0L));
        assertEquals(1L, LongUtils.safeAdd(0L, 1L));
        assertEquals(1L, LongUtils.safeAdd(1L, 0L));
        // Test near Integer.MAX_VALUE
        assertEquals(0x7FFFFFFFL, LongUtils.safeAdd(0x70000000L, 0xFFFFFFFL));
        assertEquals(0x80000000L, LongUtils.safeAdd(Integer.MAX_VALUE, 1L));
        assertEquals(0xFFFFFFFEL, LongUtils.safeAdd(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertEquals(0x100000000L, LongUtils.safeAdd(
                LongUtils.safeAdd(Integer.MAX_VALUE, 1L),
                LongUtils.safeAdd(Integer.MAX_VALUE, 1L)));
        // Test near Long.MAX_VALUE
        assertEquals(Long.MAX_VALUE, LongUtils.safeAdd(Long.MAX_VALUE, 0L));
        assertEquals(Long.MAX_VALUE, LongUtils.safeAdd(Long.MAX_VALUE, 1L));
        assertEquals(Long.MAX_VALUE, LongUtils.safeAdd(Long.MAX_VALUE, Long.MAX_VALUE));
        // Test simple math with negatives
        assertEquals(0L, LongUtils.safeAdd(-0L, 0L));
        assertEquals(0L, LongUtils.safeAdd(-0L, -0L));
        assertEquals(-1L, LongUtils.safeAdd(0L, -1L));
        // Test negative numbers combined with numbers near Integer.MAX_VALUE
        assertEquals(Long.MAX_VALUE-1, LongUtils.safeAdd(Long.MAX_VALUE, -1L));
        assertEquals(Long.MAX_VALUE-1, LongUtils.safeAdd(-1L, Long.MAX_VALUE));
        assertEquals(0xFFFFFFFFL, LongUtils.safeAdd(0x100000000L, -1L));
        assertEquals(-1L, LongUtils.safeAdd(Integer.MAX_VALUE, Integer.MIN_VALUE));
        assertEquals((long)Integer.MAX_VALUE, LongUtils.safeAdd(0x80000000L, -1L));
        // Test near Integer.MIN_VALUE
        assertEquals(Integer.MIN_VALUE, LongUtils.safeAdd(0, Integer.MIN_VALUE));
        assertEquals(-0x100000000L, LongUtils.safeAdd(Integer.MIN_VALUE, Integer.MIN_VALUE));
        // Test near Long.MIN_VALUE.  Note that overflow should always result
        // in Long.MAX_VALUE; this also applies to negative overflow.
        assertEquals(Long.MIN_VALUE, LongUtils.safeAdd(Long.MIN_VALUE, 0L));
        assertEquals(Long.MAX_VALUE, LongUtils.safeAdd(Long.MIN_VALUE, -1L));
        assertEquals(Long.MAX_VALUE, LongUtils.safeAdd(Long.MIN_VALUE, Long.MIN_VALUE));
    }
}
