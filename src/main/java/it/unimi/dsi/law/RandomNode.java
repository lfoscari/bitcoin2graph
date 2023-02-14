package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.webgraph.EFGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

import static it.unimi.dsi.law.Parameters.*;

public class RandomNode {
    public static final Logger logger = LoggerFactory.getLogger(RandomNode.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        logger.info("Loading necessary data structures...");
        Long2ObjectOpenHashMap<String> addressInverseMap = (Long2ObjectOpenHashMap<String>) BinIO.loadObject(addressesInverseMap.toFile());
        EFGraph bitcoinGraph = EFGraph.load(basename.toString());

        Random r = new Random();
        Scanner sc = new Scanner(System.in);

        while (true) {
            logger.info("Picking a random node");
            long[] addressIds = BinIO.loadLongs(ids.toFile());
            int index = r.nextInt(addressIds.length);

            long id = addressIds[index];
            String address = addressInverseMap.get(id);
            int[] successors = bitcoinGraph.successorArray(index);

            System.out.println(address + " (id: " + id + ", outdegree: " + bitcoinGraph.outdegree(index) + "):");
            for (int i = 0; i < bitcoinGraph.outdegree(index); i++) {
                long successorId = addressIds[successors[i]];
                System.out.println("\t" + addressInverseMap.get(successorId) + " (id: " + successorId + ")");
            }

            System.out.println("\nAnother? [y/n]");
            if (!sc.nextLine().equals("y")) {
                return;
            }
        }
    }
}
