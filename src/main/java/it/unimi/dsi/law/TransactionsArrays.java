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
import java.util.Arrays;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsArrays {
    private final Object2LongFunction<CharSequence> addressMap;
    private final ProgressLogger progress;

    public TransactionsArrays() throws IOException, ClassNotFoundException {
        this(null);
    }

    public TransactionsArrays(ProgressLogger progress) throws IOException, ClassNotFoundException {
        if (parsedInputsDirectory.toFile().listFiles() == null) {
            throw new NoSuchFileException("Parse inputs first with ParseTSVs");
        } else if (parsedOutputsDirectory.toFile().listFiles() == null) {
            throw new NoSuchFileException("Parse outputs first with ParseTSVs");
        } else if (!addressesMapFile.toFile().exists()) {
            throw new NoSuchFileException("Compute address map first with AddressMap");
        }

        if (progress == null) {
            Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
            progress = new ProgressLogger(logger, logInterval, logTimeUnit, "transactions");
            progress.displayLocalSpeed = true;
        }

        this.progress = progress;
        this.addressMap = (Object2LongFunction<CharSequence>) BinIO.loadObject(addressesMapFile.toFile());
    }

    void compute() throws IOException, ClassNotFoundException {
        this.progress.start("Building input transactions arrays");

        this.buildTransactionArrays(parsedInputsDirectory, inputTransactionsDirectory);
        this.buildTransactionArrays(parsedOutputsDirectory, outputTransactionsDirectory);

        this.progress.done();
    }

    private void buildTransactionArrays(Path parsedDirectory, Path transactionsDirectory) throws IOException, ClassNotFoundException {
        TSVDirectoryLineReader transactions = new TSVDirectoryLineReader(
                parsedDirectory.toFile().listFiles(),
                (line) -> true, (line) -> line, null
        );
        this.addTransactions(transactions, transactionsDirectory);
    }

    private void addTransactions(TSVDirectoryLineReader transactions, Path destinationDirectory) throws IOException, ClassNotFoundException {
        destinationDirectory.toFile().mkdir();

        while (true) {
            try {
                String[] line = transactions.next();
                String transaction = line[0], address = line[1];

                long addressLong = this.addressMap.getLong(address);
                File transactionFile = destinationDirectory.resolve(transaction).toFile();

                long[] la;
                if (transactionFile.exists()) {
                    la = (long[]) BinIO.loadObject(transactionFile);

                    boolean skip = false;
                    for (long a : la) {
                        if (a == addressLong) {
                            skip = true;
                            break;
                        }
                    }

                    if (skip) {
                        continue;
                    }

                    la = LongArrays.ensureCapacity(la, la.length + 1);
                    la[la.length - 1] = addressLong;
                } else {
                    la = new long[] { addressLong };
                }
                BinIO.storeObject(la, transactionFile);
                this.progress.lightUpdate();
            } catch (NoSuchElementException e) {
                break;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        new TransactionsArrays().compute();
    }
}
