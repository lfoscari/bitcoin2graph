package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Utils.*;

public class TransactionMap {
    static void compute() throws IOException {
        Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
        ProgressLogger progress = new ProgressLogger(logger, logInterval, logTimeUnit, "addresses");
        progress.displayLocalSpeed = true;
        progress.start("Building transaction to long map");

        {

            File[] inputs = parsedInputsDirectory.toFile().listFiles();

            if (inputs == null) {
                throw new NoSuchFileException("Parse inputs first with ParseTSVs");
            }

            TSVDirectoryLineReader inputTransactions = new TSVDirectoryLineReader(
                    parsedInputsDirectory.toFile().listFiles(),
                    (line) -> true,
                    (line) -> new String[]{line[INPUTS_IMPORTANT.indexOf(SPENDING_TRANSACTION_HASH)]},
                    progress
            );

            Object2LongFunction<String> inputTransactionsMap = parseChunks(inputTransactions);
            BinIO.storeObject(inputTransactionsMap, inputTransactionsFile.toFile());
        }

        {
            File[] outputs = parsedOutputsDirectory.toFile().listFiles();

            if (outputs == null) {
                throw new NoSuchFileException("Parse outputs first with ParseTSVs");
            }

            TSVDirectoryLineReader outputTransactions = new TSVDirectoryLineReader(
                    parsedOutputsDirectory.toFile().listFiles(),
                    (line) -> true,
                    (line) -> new String[]{line[OUTPUTS_IMPORTANT.indexOf(TRANSACTION_HASH)]},
                    progress
            );

            Object2LongFunction<String> outputTransactionsMap = parseChunks(outputTransactions);
            BinIO.storeObject(outputTransactionsMap, outputTransactionsFile.toFile());
        }
    }

    private static Object2LongFunction<String> parseChunks(TSVDirectoryLineReader transactions) {
        Object2LongFunction<String> transactionsMap = new Object2LongOpenHashMap<>();
        long count = 0;

        while (true) {
            try {
                String transaction = transactions.next()[0];
                if (!transactionsMap.containsKey(transaction)) {
                    transactionsMap.put(transaction, count++);
                }
            } catch (NoSuchElementException e) {
                break;
            }
        }

        return transactionsMap;
    }

    public static void main(String[] args) throws IOException {
        TransactionMap.compute();
    }
}
