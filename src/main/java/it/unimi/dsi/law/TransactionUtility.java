package it.unimi.dsi.law;

import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.buildInverseMap;

public class TransactionUtility {
    public static final Logger logger = LoggerFactory.getLogger(TransactionUtility.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        logger.info("Loading necessary data structures...");
        GOVMinimalPerfectHashFunction<CharSequence> transactionsMap = (GOVMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(transactionsMapFile.toFile());

        if (!transactionsInverseMapFile.toFile().exists()) {
            computeTransactionInverseMap(transactionsMap);
        }

        Object[][] transactionsInverseMap = (Object[][]) BinIO.loadObject(transactionsInverseMapFile.toFile());

        Long2ObjectOpenHashMap<LongOpenHashSet> transactionInputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionInputsFile.toFile());
        Long2ObjectOpenHashMap<LongOpenHashSet> transactionOutputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionOutputsFile.toFile());

        Scanner sc = new Scanner(System.in);
        XoRoShiRo128PlusRandom r = new XoRoShiRo128PlusRandom();

        while (true) {
            System.out.print("transaction> ");
            String transaction = sc.nextLine();

            long transactionId;

            if (transaction.equals("")) {
                System.out.println("Picking a random transaction");
                transactionId = r.nextLong(transactionsMap.size64());
            } else {
                transactionId = transactionsMap.getLong(transaction);
            }

            System.out.println(BigArrays.get(transactionsInverseMap, transactionId) + " (id: " + transactionId + ")");

            LongOpenHashSet inputs = transactionInputs.get(transactionId);
            System.out.println("Inputs (" + inputs.size() + "):");
            for (long addressId: inputs) {
                System.out.println("\t" + addressId);
            }

            LongOpenHashSet outputs = transactionOutputs.get(transactionId);
            System.out.println("Outputs (" + outputs.size() + "):");
            for (long addressId: outputs) {
                System.out.println("\t" + addressId);
            }

            System.out.print("\nAnother? [y/n] ");
            if (!sc.nextLine().equals("y")) {
                return;
            }
        }
    }

    private static void computeTransactionInverseMap(GOVMinimalPerfectHashFunction<CharSequence> transactionMap) throws IOException {
        Utils.LineFilter filter = (line) -> Utils.column(line, 7).equals("0");
        Iterator<CharSequence> transactions = Iterators.transform(Utils.readTSVs(transactionsDirectory.toFile().listFiles(), filter), line -> Utils.column(line, 1));
        ProgressLogger progress = new ProgressLogger(LoggerFactory.getLogger(TransactionUtility.class), "transactions");
        progress.expectedUpdates = transactionMap.size64();
        progress.start("Computing inverse transaction map");
        buildInverseMap(transactionMap, transactions, transactionsInverseMapFile, progress);
        progress.done();
    }
}
