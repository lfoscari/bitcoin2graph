package it.unimi.dsi.law;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Parameters {
    public final static String resources = "src/main/resources/";
    public final static List<String> CRYPTOCURRENCIES = List.of(
            "bitcoin",
            "bitcoin-cash",
            "dash",
            "dogecoin",
            "ethereum",
            "litecoin",
            "zcash"
    );

    public final static long logInterval = 10;
    public final static TimeUnit logTimeUnit = TimeUnit.SECONDS;
}