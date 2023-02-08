package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.EFGraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class TMP {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        MutableString s = new MutableString("0123456789");

        CharSequence firstFive = s.subSequence(0, 5);
        CharSequence lastFive = s.subSequence(5, s.length());

        System.out.println(s);
        System.out.println(lastFive);

        System.exit(1);

        GOVMinimalPerfectHashFunction<MutableString> transactionMap =
                (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.transactionsMap.toFile());
        GOVMinimalPerfectHashFunction<MutableString> addressMap =
                (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.addressesMap.toFile());


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

        EFGraph bitcoinGraph = EFGraph.load(Parameters.basename.toString());
        long[] ids = BinIO.loadLongs(Parameters.ids.toFile());

        Random r = new Random();
        int node = r.nextInt(ids.length);

        long id = ids[node];
        int[] successors = bitcoinGraph.successorArray(node);

        if (successors.length != bitcoinGraph.outdegree(node)) {
            throw new RuntimeException();
        }

        System.out.println(id + " (" +  findAddress(addressMap, id) + ") -> [" + bitcoinGraph.outdegree(node) + "]");
        for(int i = 0; i < bitcoinGraph.outdegree(node); i++) {
            System.out.println("\t" + ids[successors[i]] + " (" +  findAddress(addressMap, ids[successors[i]]) + ")");
        }
    }

    static MutableString findAddress(GOVMinimalPerfectHashFunction<MutableString> addressMap, long id) throws IOException {
        Iterator<MutableString> it = Utils.readTSVs(Parameters.addressesFile.toFile(), new MutableString());

        while (it.hasNext()) {
            MutableString addr = it.next();
            if (addressMap.getLong(addr) == id) {
                return addr.copy();
            }
        }

        return null;
    }
}
