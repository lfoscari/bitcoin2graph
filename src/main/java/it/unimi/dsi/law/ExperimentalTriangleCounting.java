package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class ExperimentalTriangleCounting {
    private static final Logger logger = LoggerFactory.getLogger(ExperimentalTriangleCounting.class);
    private static final ProgressLogger pl = new ProgressLogger(logger);

    public static void main(String[] args) throws JSAPException, IOException {
        final SimpleJSAP jsap = new SimpleJSAP(ExperimentalTriangleCounting.class.getName(), "Magical clustering coefficient computation",
            new Parameter[] {
                new Switch("test", 't', "test", "Run a battery of tests."),
                new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, false, "Basename for the graph.")
        });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        if (jsapResult.getBoolean("test")) {
            System.out.println("RUNNING TESTS\n");
            runTests();
            System.exit(1);
        }

        String basename = jsapResult.getString("basename");
        ImmutableGraph graph = ImmutableGraph.load(basename);

        int[] triangles = countTriangles(graph);

        for (int node = 0; node < triangles.length; node++) {
            System.out.println(node + ": " + triangles[node]);
        }
    }

    private static void runTests() {
        ImmutableGraph g = ArrayListMutableGraph.newCompleteGraph(5, false).immutableView();
        System.out.println(g.toString());

        int[] triangles = countTriangles(g);
        for (int node = 0; node < triangles.length; node++) {
            System.out.println(node + ": " + triangles[node]);
        }
    }

    static int[] countTriangles(ImmutableGraph graph) {
        /* Initialization */
        // TODO: use IntArrays

        int[] triangles = new int[graph.numNodes()];
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
                if (labels[permutation[node + i]] == node) {
                    triangles[node]++;
                }
            }
        }

        pl.done();
        return triangles;
    }

    private static void propagateLabels(ImmutableGraph graph, int[] labels) {
        pl.expectedUpdates = labels.length;

        int index = 0;
        for (NodeIterator it = graph.nodeIterator(); it.hasNext(); ) {
            int node = it.nextInt();
            int outdegree = it.outdegree();
            if (outdegree <= 0) continue;

            int[] successors = it.successorArray();
            System.arraycopy(successors, 0, labels, index, outdegree);

            pl.update(outdegree);
            index += outdegree;
        }
    }
}
