package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Scanner;

import static it.unimi.dsi.law.Parameters.*;

public class TransactionUtility {
    public static final Logger logger = LoggerFactory.getLogger(TransactionUtility.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        logger.info("Loading necessary data structures...");
        GOVMinimalPerfectHashFunction<CharSequence> transactionsMap = (GOVMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(transactionsMapFile.toFile());
        Object[][] addressInverseMap = (Object[][]) BinIO.loadObject(addressesInverseMapFile.toFile());

        Long2ObjectOpenHashMap<LongOpenHashSet> transactionInputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionInputsFile.toFile());
        Long2ObjectOpenHashMap<LongOpenHashSet> transactionOutputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionOutputsFile.toFile());

        Scanner sc = new Scanner(System.in);
        XoRoShiRo128PlusRandom r = new XoRoShiRo128PlusRandom();

        while (true) {
            System.out.print("transaction> ");

            String transaction;
            try {
                transaction = sc.nextLine();
            } catch (NoSuchElementException e) {
                return;
            }

            long transactionId;

            if (transaction.equals("")) {
                System.out.println("Picking a random transaction");
                transactionId = r.nextLong(transactionsMap.size64());
            } else {
                transactionId = transactionsMap.getLong(transaction);
            }

            System.out.println(transaction + " (id: " + transactionId + ")");

            LongOpenHashSet inputs = transactionInputs.get(transactionId);
            System.out.println("Inputs (" + inputs.size() + "):");
            inputs.forEach(id -> System.out.println("\t" + BigArrays.get(addressInverseMap, id)));

            LongOpenHashSet outputs = transactionOutputs.get(transactionId);
            System.out.println("Outputs (" + outputs.size() + "):");
            outputs.forEach(id -> System.out.println("\t" + BigArrays.get(addressInverseMap, id)));
        }
    }
}
