package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.EFGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import org.apache.commons.lang3.mutable.Mutable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import static it.unimi.dsi.law.Parameters.transactionsDirectory;

public class TMP {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Utils.LineCleaner cleaner = (line) -> Utils.column(line, 1);
        Utils.LineFilter filter = (line) -> Utils.columnEquals(line, 7, "0");
        Iterator<MutableString> it = Utils.readTSVs(transactionsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv")), new MutableString(), filter, cleaner);
        GOVMinimalPerfectHashFunction<MutableString> transactionMap = (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.transactionsMap.toFile());

        it.forEachRemaining((t) -> {
            System.out.println(t + " => " + transactionMap.getLong(t));
            if (transactionMap.getLong(t) == transactionMap.defaultReturnValue()) {
                throw new RuntimeException();
            }
        });

        EFGraph bitcoinGraph = EFGraph.load(Parameters.basename.toString());
        long[] ids = BinIO.loadLongs(Parameters.ids.toFile());

        int node = 56;
        long id = ids[node];
        int[] successors = bitcoinGraph.successorArray(node);

        System.out.println(id + " (" +  findAddress(id) + ") -> [" + bitcoinGraph.outdegree(node) + "]");
        for(int i = 0; i < bitcoinGraph.outdegree(node); i++) {
            System.out.println("\t" + ids[successors[i]] + " (" +  findAddress(ids[successors[i]]) + ")");
        }
    }

    static MutableString findAddress(long id) throws IOException, ClassNotFoundException {
        Iterator<MutableString> it = Utils.readTSVs(Parameters.addressesFile.toFile(), new MutableString(), null, null);
        GOVMinimalPerfectHashFunction<MutableString> addressMap = (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.addressesMap.toFile());

        while (it.hasNext()) {
            MutableString addr = it.next();
            if (addressMap.getLong(addr) == id) {
                return addr;
            }
        }

        return null;
    }
}
