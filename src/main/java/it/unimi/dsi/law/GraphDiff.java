package it.unimi.dsi.law;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class GraphDiff {
    public static void main(String[] args) throws IOException {
        ProgressLogger pl = new ProgressLogger(LoggerFactory.getLogger(GraphDiff.class), "nodes");

        Scanner sc = new Scanner(System.in);
        System.out.print("basename first graph: ");
        String g1b = sc.nextLine();

        System.out.print("basename second graph: ");
        String g2b = sc.nextLine();

        pl.logger.info("Loading first graph");
        BVGraph g1 = BVGraph.loadOffline(g1b, pl);
        pl.logger.info("Loading second graph");
        BVGraph g2 = BVGraph.loadOffline(g2b, pl);

        pl.expectedUpdates = Integer.min(g1.numNodes(), g2.numNodes());

        pl.start("Checking nodes");

        NodeIterator g1it = g1.nodeIterator();
        NodeIterator g2it = g2.nodeIterator();

        while (true) {
            int g1n = g1it.nextInt();
            int g2n = g2it.nextInt();

            pl.lightUpdate();

            if (g1n == -1 || g2n == -1) {
                pl.logger.info("Terminating " + g1n + " " + g2n);
                break;
            }

            if (g1n != g2n) {
                pl.logger.error("Node mismatch: " + g1n + " != " + g2n);
                continue;
            }

            int[] g1suc = g1it.successorArray();
            int[] g2suc = g2it.successorArray();

            for (int i = 0; i < Math.min(g1suc.length, g2suc.length); i++) {
                if (g1suc[i] != g2suc[i]) {
                    pl.logger.error("Successor mismatch for node " + g1n + ":\n\t" + Arrays.toString(g1suc) + "\n\t" + Arrays.toString(g2suc));
                }
            }

            if (g1suc.length != g2suc.length) {
                pl.logger.error("Successor mismatch for node " + g1n + ":\n\t" + Arrays.toString(g1suc) + "\n\t" + Arrays.toString(g2suc));
            }
        }

        pl.done();
    }
}
