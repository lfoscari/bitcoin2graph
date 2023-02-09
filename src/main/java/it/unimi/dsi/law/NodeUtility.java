package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.EFGraph;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Scanner;

import static it.unimi.dsi.law.Parameters.*;

public class NodeUtility {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        GOVMinimalPerfectHashFunction<MutableString> addressMap =
                (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(addressesMap.toFile());
        Long2ObjectOpenHashMap<String> addressInverseMap =
                (Long2ObjectOpenHashMap<String>) BinIO.loadObject(addressesInverseMap.toFile());

        EFGraph bitcoinGraph = EFGraph.load(basename.toString());
        long[] addressIds = BinIO.loadLongs(ids.toFile());

        Scanner sc = new Scanner(System.in);
        System.out.print("address> ");
        String address = sc.nextLine();

        long id = addressMap.getLong(address);
        int node = ArrayUtils.indexOf(addressIds, id);

        int[] successors = bitcoinGraph.successorArray(node);

        System.out.println(address + " (id: " + id + ", outdegree: " + bitcoinGraph.outdegree(node) + "):");
        for(int i = 0; i < bitcoinGraph.outdegree(node); i++) {
            long successorId = addressIds[successors[i]];
            System.out.println("\t" + addressInverseMap.get(successorId) + " (id: " +  successorId + ")");
        }
    }
}
