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

        int[] triangles = triangles(graph);

        for (int node = 0; node < triangles.length; node++) {
            System.out.println(node + ": " + triangles[node]);
        }
    }

    private static void runTests() {
        ImmutableGraph graph = ArrayListMutableGraph.newCompleteGraph(3, false).immutableView();
        System.out.println(graph);

        int[] triangles = triangles(graph);
        int[] count = new int[graph.numNodes()];

        int index = 0;
        NodeIterator it = graph.nodeIterator();
        while (it.hasNext()) {
            int node = it.nextInt();
            int outdegree = it.outdegree();

            for (int i = 0; i < outdegree; i++) {
                if (triangles[index + i] == node) {
                    count[node]++;
                }
            }

            index += outdegree;
        }

        System.out.println("count " + Arrays.toString(count));
    }

    static int[] triangles(ImmutableGraph graph) {
        int[] permutation = new int[(int) graph.numArcs()];
        Arrays.setAll(permutation, (i) -> i);

        int[] neighbourhood = initNeighbourhood(graph);
        System.out.println("neighbours " + Arrays.toString(neighbourhood));

        IntArrays.parallelQuickSortIndirect(permutation, neighbourhood);

        int[] labels = propagateLabels(graph, neighbourhood, permutation);
        System.out.println("labels " + Arrays.toString(labels));

        return labels;
    }

    private static int[] propagateLabels(ImmutableGraph graph, int[] neighbourhood, int[] permutation) {
        int[] labels = new int[(int) graph.numArcs()];

        int index = 0;
        NodeIterator it = graph.nodeIterator();

        while (it.hasNext()) {
            int node = it.nextInt();
            int outdegree = it.outdegree();

            for (int i = 0; i < outdegree; i++) {
                labels[index + i] = neighbourhood[permutation[permutation[permutation[index + i]]]];
            }

            index += outdegree;
        }

        return labels;
    }

    private static int[] initNeighbourhood(ImmutableGraph graph) {
        int[] neighbour = new int[(int) graph.numArcs()];

        int index = 0;
        NodeIterator it = graph.nodeIterator();

        while (it.hasNext()) {
            it.nextInt();
            int outdegree = it.outdegree();
            int[] successors = it.successorArray();

            for (int i = 0; i < outdegree; i++) {
                neighbour[index + i] = successors[i];
            }

            index += outdegree;
        }

        return neighbour;
    }
}
