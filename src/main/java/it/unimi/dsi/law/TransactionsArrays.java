package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsArrays {
    static void compute() throws IOException, ClassNotFoundException {
        Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
        ProgressLogger progress = new ProgressLogger(logger, logInterval, logTimeUnit, "___");
        progress.displayLocalSpeed = true;

        if (parsedInputsDirectory.toFile().listFiles() == null) {
            throw new NoSuchFileException("Parse inputs first with ParseTSVs");
        } else if (parsedOutputsDirectory.toFile().listFiles() == null) {
            throw new NoSuchFileException("Parse outputs first with ParseTSVs");
        } else if (!addressesMapFile.toFile().exists()) {
            throw new NoSuchFileException("Compute address map first with AddressMap");
        }

        Object2LongFunction<CharSequence> addressMap =
                (Object2LongFunction<CharSequence>) BinIO.loadObject(addressesMapFile.toFile());

        progress.start("Building input transactions arrays");

        TSVDirectoryLineReader transactions = new TSVDirectoryLineReader(
                parsedInputsDirectory.toFile().listFiles(),
                (line) -> true, (line) -> line, progress
        );
        addTransactions(transactions, inputTransactionsDirectory, addressMap, progress);

        transactions = new TSVDirectoryLineReader(
                parsedInputsDirectory.toFile().listFiles(),
                (line) -> true, (line) -> line, progress
        );
        addTransactions(transactions, outputTransactionsDirectory, addressMap, progress);
    }

    private static void addTransactions(TSVDirectoryLineReader transactions, Path destinationDirectory, Object2LongFunction<CharSequence> addressMap, ProgressLogger progress) throws IOException, ClassNotFoundException {
        destinationDirectory.toFile().mkdir();

        while (true) {
            try {
                String[] line = transactions.next();
                String transaction = line[0], address = line[1];

                long addressLong = addressMap.getLong(address);

                File transactionFile = destinationDirectory.resolve(transaction).toFile();
                long[] la;

                if (transactionFile.exists()) {
                    la = (long[]) BinIO.loadObject(transactionFile);
                    la = LongArrays.ensureCapacity(la, la.length + 1);
                    la[la.length - 1] = addressLong;
                } else {
                    la = new long[] { addressLong };
                }

                BinIO.storeObject(la, transactionFile);
                progress.lightUpdate();
            } catch (NoSuchElementException e) {
                break;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        TransactionsArrays.compute();
    }
}
