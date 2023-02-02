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

public class TMP {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Utils.LineCleaner cleaner = (s) -> Utils.column(s, 1);
        Utils.LineFilter filter = (s) -> Utils.column(s, 7).equals("0");
        Iterable<MutableString> it = Utils.readTSVs(Parameters.transactionsDirectory.toFile().listFiles(), new MutableString(), filter, cleaner);
        GOVMinimalPerfectHashFunction<MutableString> transactionMap = (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.transactionsMap.toFile());

        it.forEach((t) -> {
            if (transactionMap.getLong(t) == transactionMap.defaultReturnValue()) {
                throw new RuntimeException();
            }
        });

        EFGraph bitcoinGraph = EFGraph.load(Parameters.basename.toString());
        long[] ids = BinIO.loadLongs(Parameters.ids.toFile());

        int node = 234;
        int[] successors = bitcoinGraph.successorArray(node);

        System.out.println(ids[node] + " (" +  findAddress(ids[node]) + ") -> [" + bitcoinGraph.outdegree(234) + "]");
        for(int i = 0; i < bitcoinGraph.outdegree(234); i++) {
            System.out.println("\t" + ids[successors[i]] + " (" +  findAddress(ids[successors[i]]) + ")");
        }
    }

    static MutableString findAddress(long id) throws IOException, ClassNotFoundException {
        Iterable<MutableString> it = Utils.readTSVs(Parameters.addressesFile.toFile(), new MutableString(), null, null);
        GOVMinimalPerfectHashFunction<MutableString> addressMap = (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.addressesMap.toFile());

        for (MutableString addr : it) {
            if (addressMap.getLong(addr) == id) {
                return addr;
            }
        }

        return null;
    }
}
