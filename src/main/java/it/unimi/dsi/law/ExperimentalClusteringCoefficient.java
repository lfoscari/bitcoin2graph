package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusPlusRandom;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.checkerframework.common.reflection.qual.ClassValBottom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class ExperimentalClusteringCoefficient {
    private static final XoRoShiRo128PlusPlusRandom r = new XoRoShiRo128PlusPlusRandom();
    private static final Logger logger = LoggerFactory.getLogger(ExperimentalClusteringCoefficient.class);
    private static final ProgressLogger pl = new ProgressLogger(logger);

    public static void main(String[] args) throws JSAPException, IOException {
        final SimpleJSAP jsap = new SimpleJSAP(ExperimentalClusteringCoefficient.class.getName(), "Magical clustering coefficient computation",
            new Parameter[] {
                new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, false, "Basename for the graph.")
        });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        String basename = jsapResult.getString("basename");
        ImmutableGraph graph = ImmutableGraph.load(basename);

        /* Initialization */
        // TODO: use IntArrays

        int[] labels = new int[(int) graph.numArcs() * 2];

        int[] permutation = new int[labels.length];
        Arrays.setAll(permutation, (i) -> i);

        /* First propagation */
        pl.start("Starting first propagation");
        propagateLabels(graph, labels);
        pl.start("Sorting labels");
        IntArrays.parallelQuickSortIndirect(permutation, labels);

        /* Second propagation */
        pl.start("Starting second propagation");
        propagateLabels(graph, labels);
        pl.start("Sorting labels");
        IntArrays.parallelQuickSortIndirect(permutation, labels);

        /* Third propagation */
        pl.start("Starting third propagation");
        propagateLabels(graph, labels);

        /* Summing up */
        pl.start("Counting triangles");
        for (NodeIterator it = graph.nodeIterator(); it.hasNext(); ) {
            int node = it.nextInt();

            for (int i = 0; i < it.outdegree(); i++) {
                if (labels[permutation[i]] == node) {
                    System.out.println(node + " is in a triangle");
                }
            }
        }

        pl.done();
    }

    private static void propagateLabels(ImmutableGraph graph, int[] labels) {
        pl.expectedUpdates = labels.length;
        int index = 0;
        for (NodeIterator it = graph.nodeIterator(); it.hasNext(); it.nextInt()) {
            int outdegree = it.outdegree();
            int[] successors = it.successorArray();

            for (int i = 0; i < outdegree; i++) {
                labels[index + i] = successors[i];
                pl.lightUpdate();
            }

            index += outdegree;
        }
    }
}
