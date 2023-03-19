package it.unimi.dsi.law;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class GraphDiff {
    public static void main(String[] args) throws IOException {
        ProgressLogger pl = new ProgressLogger("nodes");

        Scanner sc = new Scanner(System.in);
        System.out.print("basename first graph: ");
        String g1b = sc.nextLine();

        System.out.print("basename second graph: ");
        String g2b = sc.nextLine();

        BVGraph g1 = BVGraph.load(g1b);
        BVGraph g2 = BVGraph.load(g2b);

        pl.expectedUpdates = Integer.min(g1.numNodes(), g2.numNodes());

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
    }
}
