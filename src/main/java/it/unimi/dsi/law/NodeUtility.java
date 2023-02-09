package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.EFGraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import static it.unimi.dsi.law.Parameters.*;

public class NodeUtility {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        GOVMinimalPerfectHashFunction<MutableString> addressMap =
                (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(addressesMap.toFile());

        EFGraph bitcoinGraph = EFGraph.load(basename.toString());
        long[] addressIds = BinIO.loadLongs(ids.toFile());

        Random r = new Random();
        int node = r.nextInt(addressIds.length);

        long id = addressIds[node];
        int[] successors = bitcoinGraph.successorArray(node);

        System.out.println(findAddress(addressMap, id) + " (id: " + id + ", outdegree: " + bitcoinGraph.outdegree(node) + "):");
        for(int i = 0; i < bitcoinGraph.outdegree(node); i++) {
            System.out.println("\t" + findAddress(addressMap, addressIds[successors[i]]) + " (id: " +  addressIds[successors[i]] + ")");
        }
    }

    static MutableString findAddress(GOVMinimalPerfectHashFunction<MutableString> addressMap, long id) {
        Iterator<MutableString> it = Utils.readTSVs(addressesFile);

        while (it.hasNext()) {
            MutableString addr = it.next();
            if (addressMap.getLong(addr) == id) {
                return addr.copy();
            }
        }

        return null;
    }
}
