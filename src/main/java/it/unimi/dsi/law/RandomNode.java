package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.webgraph.EFGraph;

import java.io.IOException;
import java.util.Random;

import static it.unimi.dsi.law.Parameters.*;

public class RandomNode {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Long2ObjectOpenHashMap<String> addressInverseMap =
                (Long2ObjectOpenHashMap<String>) BinIO.loadObject(addressesInverseMap.toFile());

        EFGraph bitcoinGraph = EFGraph.load(basename.toString());
        long[] addressIds = BinIO.loadLongs(ids.toFile());
        int index = new Random().nextInt(addressIds.length);

        long id = addressIds[index];
        String address = addressInverseMap.get(id);
        int[] successors = bitcoinGraph.successorArray(index);

        System.out.println(address + " (id: " + id + ", outdegree: " + bitcoinGraph.outdegree(index) + "):");
        for (int i = 0; i < bitcoinGraph.outdegree(index); i++) {
            long successorId = addressIds[successors[i]];
            System.out.println("\t" + addressInverseMap.get(successorId) + " (id: " + successorId + ")");
        }
        System.out.println();
    }
}
