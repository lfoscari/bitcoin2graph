package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.algo.HyperBall;

import java.io.IOException;

public class Metrics {
    private static final int SEED = 91;
    private static final ProgressLogger pl = new ProgressLogger();

    public static void main(String[] args) throws JSAPException, IOException {
        final SimpleJSAP jsap = new SimpleJSAP(Compress.class.getName(), "Compute various metrics on the given graph",
                new Parameter[] {
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
                        new FlaggedOption("transposed-basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "The basename of the transposed graph."),
                        new FlaggedOption("batch-size", JSAP.STRING_PARSER, "10000000", JSAP.NOT_REQUIRED, 'b', "The batch size to compute the transpose of the given graph."),
                }
        );

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        ImmutableGraph graph = ImmutableGraph.loadOffline(jsapResult.getString("basename"), pl);
        ImmutableGraph tgraph = jsapResult.contains("transposed-basename") ?
            ImmutableGraph.loadOffline(jsapResult.getString("transposed-basename"), pl) :
            Transform.transposeOffline(graph, jsapResult.getInt("batch-size"));

        try (HyperBall hb = new HyperBall(graph, tgraph, 128, pl, 0, 0, 0, true)) {
            hb.run(100, SEED);
        }
    }
}
