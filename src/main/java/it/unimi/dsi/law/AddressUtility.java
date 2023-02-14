package it.unimi.dsi.law;

import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.EFGraph;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.buildInverseMap;

public class AddressUtility {
    public static final Logger logger = LoggerFactory.getLogger(AddressUtility.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        logger.info("Loading necessary data structures...");
        GOVMinimalPerfectHashFunction<CharSequence> addressMap = (GOVMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(addressesMap.toFile());

        if (!addressesInverseMap.toFile().exists()) {
            computeAddressInverseMap(addressMap);
        }

        Object[][] addressInverseMap = (Object[][]) BinIO.loadObject(addressesInverseMap.toFile());

        EFGraph bitcoinGraph = EFGraph.load(basename.toString());
        long[] addressIds = BinIO.loadLongs(ids.toFile());

        XoRoShiRo128PlusRandom r = new XoRoShiRo128PlusRandom();
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.print("address> ");
            String address = sc.nextLine();

            long id;

            if (address.equals("")) {
                System.out.println("Picking a random address");
                id = r.nextLong(addressMap.size64());

                if (!BigArrays.get(addressInverseMap, id).equals(address)) {
                    logger.error("Address not found!");
                    continue;
                }
            } else {
                id = addressMap.getLong(address);
            }

            int node = ArrayUtils.indexOf(addressIds, id);
            int[] successors = bitcoinGraph.successorArray(node);

            System.out.println(BigArrays.get(addressInverseMap, id) + " (id: " + id + ", outdegree: " + bitcoinGraph.outdegree(node) + "):");
            for (int i = 0; i < bitcoinGraph.outdegree(node); i++) {
                long successorId = addressIds[successors[i]];
                System.out.println("\t" + BigArrays.get(addressInverseMap, successorId) + " (id: " + successorId + ")");
            }
            System.out.println();
        }
    }

    private static void computeAddressInverseMap(GOVMinimalPerfectHashFunction<CharSequence> addressMap) throws IOException {
        Iterator<CharSequence> addresses = Iterators.transform(Utils.readTSVs(addressesFile), line -> Utils.column(line, 0));
        ProgressLogger progress = new ProgressLogger(LoggerFactory.getLogger(AddressUtility.class), "addresses");
        progress.expectedUpdates = addressMap.size64();
        progress.start("Computing inverse address map");
        buildInverseMap(addressMap, addresses, addressesInverseMap, progress);
        progress.done();
    }
}
