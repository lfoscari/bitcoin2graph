package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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

    private static void runTests() throws IOException {
        // ImmutableGraph graph = ArrayListMutableGraph.newCompleteGraph(3, false).immutableView();
        ImmutableGraph graph = new ScatteredArcsASCIIGraph(new ByteArrayInputStream("0 1\n1 2\n1 3\n2 3".getBytes()),
                true);
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
        int[] v = new int[(int) graph.numArcs()];

        NodeIterator it = graph.nodeIterator();
        int index = 0;

        while (it.hasNext()) {
            int node = it.nextInt();
            int outdegree = it.outdegree();

            for (int i = 0; i < outdegree; i++) {
                v[index + i] = node;
            }

            index += outdegree;
        }

        System.out.println("0) v " + Arrays.toString(v));

        // First propagation

        it = graph.nodeIterator();
        index = 0;

        while (it.hasNext()) {
            int node = it.nextInt();
            int outdegree = it.outdegree();
            int[] successors = it.successorArray();

            for (int i = 0; i < outdegree; i++) {
                v[index + i] = successors[i];
            }

            index += outdegree;
        }

        int[] firstPermutation = new int[(int) graph.numArcs()];
        Arrays.setAll(firstPermutation, (i) -> i);

        IntArrays.quickSortIndirect(firstPermutation, v);
        for (int i = 0; i < v.length; i++) v[i] = v[firstPermutation[i]];

        System.out.println("1) v " + Arrays.toString(v));

        // Second propagation

        it = graph.nodeIterator();
        index = 0;

        while (it.hasNext()) {
            int node = it.nextInt();
            int outdegree = it.outdegree();
            int[] successors = it.successorArray();

            for (int i = 0; i < outdegree; i++) {
                v[index + i] = successors[i];
            }

            index += outdegree;
        }

        int[] secondPermutation = new int[(int) graph.numArcs()];
        Arrays.setAll(secondPermutation, (i) -> i);

        IntArrays.quickSortIndirect(secondPermutation, v);
        for (int i = 0; i < v.length; i++) v[i] = v[secondPermutation[i]];

        System.out.println("2) v " + Arrays.toString(v));

        // Third propagation

        it = graph.nodeIterator();
        index = 0;

        while (it.hasNext()) {
            int node = it.nextInt();
            int outdegree = it.outdegree();
            int[] successors = it.successorArray();

            for (int i = 0; i < outdegree; i++) {
                v[index + i] = successors[i];
            }

            index += outdegree;
        }

        int[] thirdPermutation = new int[(int) graph.numArcs()];
        Arrays.setAll(thirdPermutation, (i) -> i);

        IntArrays.quickSortIndirect(thirdPermutation, v);
        for (int i = 0; i < v.length; i++) v[i] = v[thirdPermutation[i]];

        System.out.println("3) v " + Arrays.toString(v));

        return v;
    }
}
