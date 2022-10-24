package it.unimi.dsi.law;

import org.rocksdb.util.SizeUnit;

import java.util.concurrent.TimeUnit;

public class Parameters {
    public final static String resources = "src/main/resources/";
    public final static String basename = "bitcoin";
    public final static long logInterval = 10;
    public final static TimeUnit logTimeUnit = TimeUnit.SECONDS;

    /* ROCKSDB */
    public final static long WRITE_BUFFER_SIZE = 64 * SizeUnit.MB;
    public final static long MAX_TOTAL_WAL_SIZE = SizeUnit.GB;
    public final static int MAX_BACKGROUND_JOBS = 10;
}