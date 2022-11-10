package it.unimi.dsi.law;

import org.rocksdb.util.SizeUnit;

import java.util.concurrent.TimeUnit;

public class Parameters {
    public final static String resources = "src/main/resources/";
    public final static String basename = "bitcoin";
    public static final long COINBASE_ADDRESS = 0L;
    public final static long logInterval = 10;
    public final static TimeUnit logTimeUnit = TimeUnit.SECONDS;

    // Each block file is roughly 128MB and each thread works on a single block file
    public final static int numberOfThreads = 2; // Math.max(1, (int) (Runtime.getRuntime().maxMemory() / (128 * SizeUnit.MB)) - 1);

    /* ROCKSDB */
    public final static long WRITE_BUFFER_SIZE = 64 * SizeUnit.MB;
    public final static long MAX_TOTAL_WAL_SIZE = SizeUnit.GB;
    public final static int MAX_BACKGROUND_JOBS = 5;
    public final static long MAX_BYTES_FOR_LEVEL_BASE = SizeUnit.GB;
}