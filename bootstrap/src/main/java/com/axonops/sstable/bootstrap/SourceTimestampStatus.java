package com.axonops.sstable.bootstrap;

import java.io.PrintStream;

/** Reports imported timestamp bounds without loading Cassandra metadata classes. */
final class SourceTimestampStatus {
    static final String MANIFEST_KEY = "source.max-timestamp-micros";

    private SourceTimestampStatus() {
    }

    static long currentTimeMicros() {
        return System.currentTimeMillis() * 1000L;
    }

    static void print(long sourceMaximumMicros,
                      long controllerNowMicros,
                      PrintStream out) {
        out.println("source.maxTimestampMicros=" + sourceMaximumMicros);
        out.println("controller.nowMicros=" + controllerNowMicros);
        if (sourceMaximumMicros > controllerNowMicros) {
            out.println("warning.futureSourceTimestamp=true");
            if (sourceMaximumMicros == Long.MAX_VALUE) {
                out.println("WARNING: imported SSTables contain the maximum Cassandra "
                        + "timestamp; default wall-clock INSERT/UPDATE values cannot "
                        + "supersede it with a greater timestamp.");
            } else {
                out.println("WARNING: imported SSTables contain a timestamp ahead of the "
                        + "controller clock; default wall-clock INSERT/UPDATE values may not "
                        + "win. Use an explicit USING TIMESTAMP greater than "
                        + sourceMaximumMicros + ".");
            }
        }
    }
}
