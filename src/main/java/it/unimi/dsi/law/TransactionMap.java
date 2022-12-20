package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.law.Utils.LineCleaner;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.SPENDING_TRANSACTION_HASH;

public class TransactionMap {
    void compute() throws IOException {
        Object2IntFunction<String> transactionMap = new Object2IntOpenHashMap<>();
        int count = 1;

        Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
        ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "transactions");
        progress.start("Building transaction to long map");

        File[] inputs = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith("tsv"));

        if (inputs == null) {
            throw new NoSuchFileException("Download inputs first");
        }

        LineCleaner cleaner = (line) -> Utils.keepColumns(line, List.of(SPENDING_TRANSACTION_HASH));

        for (String[] transactionLine : Utils.readTSVs(inputs, null, cleaner)) {
            if (!transactionMap.containsKey(transactionLine[0])) {
                transactionMap.put(transactionLine[0], count);
                count = Math.addExact(count, 1);
                progress.lightUpdate();
            }
        }

        progress.start("Saving transactions (total " + count + " transactions)");
        BinIO.storeObject(transactionMap, transactionMapLocation.toFile());
        progress.stop("Map saved in " + transactionMapLocation);
    }

    public static void main (String[] args) throws IOException {
        new TransactionMap().compute();
    }
}