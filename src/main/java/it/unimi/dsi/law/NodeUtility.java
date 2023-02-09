package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.EFGraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class NodeUtility {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        GOVMinimalPerfectHashFunction<MutableString> addressMap =
                (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.addressesMap.toFile());

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

    static MutableString findAddress(GOVMinimalPerfectHashFunction<MutableString> addressMap, long id) {
        Iterator<MutableString> it = Utils.readTSVs(Parameters.addressesFile);

        while (it.hasNext()) {
            MutableString addr = it.next();
            if (addressMap.getLong(addr) == id) {
                return addr.copy();
            }
        }

        return null;
    }
}
