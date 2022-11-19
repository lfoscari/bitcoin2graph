package it.unimi.dsi.law;

import com.google.common.primitives.Bytes;
import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.LegacyAddress;
import org.rocksdb.util.SizeUnit;

import java.util.concurrent.TimeUnit;

public class Parameters {
    public final static String resources = "src/main/resources/";
    public final static String basename = resources + "ScatteredArcsASCIIGraph/bitcoin";

    public static final byte[] UNKNOWN_ADDRESS = ByteConversion.long2bytes(-1);
    public static final byte[] MISSING_ADDRESS = ByteConversion.long2bytes(-2);


    public final static long logInterval = 10;
    public final static TimeUnit logTimeUnit = TimeUnit.SECONDS;

    public final static int numberOfThreads = 2;

    /* ROCKSDB */
    public final static long WRITE_BUFFER_SIZE = 64 * SizeUnit.MB;
    public final static long MAX_TOTAL_WAL_SIZE = SizeUnit.GB;
    public final static int MAX_BACKGROUND_JOBS = 5;
    public final static long MAX_BYTES_FOR_LEVEL_BASE = SizeUnit.GB;
}