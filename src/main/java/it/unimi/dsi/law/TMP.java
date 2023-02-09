package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.EFGraph;

import java.awt.print.PrinterAbortException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class TMP {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        /* FileLinesMutableStringIterable it = new FileLinesMutableStringIterable(Parameters.addressesFile.toString());
        for (MutableString s: it) {
            System.out.println(s);
        }

        System.exit(1);

        Long2ObjectOpenHashMap<LongOpenHashSet> transactionInputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(Parameters.transactionInputsFile.toFile());
        transactionInputs.trim();
        BinIO.storeObject(transactionInputs, Parameters.transactionInputsFile.toFile());

        System.gc();

        Long2ObjectOpenHashMap<LongOpenHashSet> transactionOutputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(Parameters.transactionOutputsFile.toFile());
        transactionOutputs.trim();
        BinIO.storeObject(transactionOutputs, Parameters.transactionOutputsFile.toFile());

        System.exit(1); */

        /* MutableString s = new MutableString("0123456789");

        CharSequence firstFive = s.subSequence(0, 5);
        CharSequence lastFive = s.subSequence(5, s.length());

        System.out.println(s);
        System.out.println(lastFive);

        System.exit(1);*/


        /*Iterator<MutableString> it = Utils.readTSVs(resources.resolve("test.tsv").toFile(), new MutableString(), null, null);
        while (it.hasNext()) {
            MutableString s = it.next();
            System.out.println(transactionMap.getLong(Utils.column(s, 0)) + ", " + addressMap.getLong(Utils.column(s, 1)));
        } */

        /*Long2ObjectOpenHashMap<LongList> test = new Long2ObjectOpenHashMap<>();

        test.put(5, LongArrayList.of(9));
        test.compute(5, (k, v) -> {
            if (v == null) {
                return LongArrayList.of(10);
            }

            v.add(10);
            return v;
        });

        test.compute(3, (k, v) -> {
            if (v == null) {
                return LongArrayList.of(10);
            }

            v.add(10);
            return v;
        });

        System.out.println(test); */


        /* MutableString s = new MutableString("ciao\tcome\tva");
        System.out.println(Utils.column(s, 0));
        System.out.println(Utils.column(s, 1));
        System.out.println(Utils.column(s, 2));
        System.out.println(s); */

        /* Utils.LineCleaner cleaner = (line) -> Utils.column(line, 1);
        Utils.LineFilter filter = (line) -> Utils.columnEquals(line, 7, "0");
        Iterator<MutableString> it = Utils.readTSVs(transactionsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv")), new MutableString(), filter, cleaner);

        it.forEachRemaining((t) -> {
            System.out.println(t + " => " + transactionMap.getLong(t));
            if (transactionMap.getLong(t) == transactionMap.defaultReturnValue()) {
                throw new RuntimeException();
            }
        }); */
    }
}
