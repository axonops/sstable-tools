package com.csforge.sstable.bootstrap;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

public class SourceTimestampStatusTest {
    @Test
    public void warnsOnlyWhenSourceMaximumIsAheadOfControllerClock() throws Exception {
        ByteArrayOutputStream futureBytes = new ByteArrayOutputStream();
        SourceTimestampStatus.print(200L, 100L, new PrintStream(futureBytes, true, "UTF-8"));
        String future = futureBytes.toString("UTF-8");
        Assert.assertTrue(future.contains("source.maxTimestampMicros=200"));
        Assert.assertTrue(future.contains("controller.nowMicros=100"));
        Assert.assertTrue(future.contains("warning.futureSourceTimestamp=true"));
        Assert.assertTrue(future.contains("USING TIMESTAMP greater than 200"));

        ByteArrayOutputStream currentBytes = new ByteArrayOutputStream();
        SourceTimestampStatus.print(100L, 100L,
                new PrintStream(currentBytes, true, "UTF-8"));
        String current = currentBytes.toString("UTF-8");
        Assert.assertTrue(current.contains("source.maxTimestampMicros=100"));
        Assert.assertFalse(current.contains("warning.futureSourceTimestamp"));

        ByteArrayOutputStream maximumBytes = new ByteArrayOutputStream();
        SourceTimestampStatus.print(Long.MAX_VALUE, 100L,
                new PrintStream(maximumBytes, true, "UTF-8"));
        Assert.assertTrue(maximumBytes.toString("UTF-8")
                .contains("cannot supersede it with a greater timestamp"));
    }
}
