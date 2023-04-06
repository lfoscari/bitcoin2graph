package it.unimi.dsi.law;

import it.unimi.dsi.law.graph.LayeredLabelPropagation;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;

import java.io.IOException;

import static it.unimi.dsi.law.Parameters.*;

public class Compress {
    private static final int SEED = 33;
    private static final ProgressLogger pl = new ProgressLogger();

    public static void main(String[] args) throws IOException {
        pl.logger.info("Loading graph");
        ImmutableGraph graph = ImmutableGraph.loadMapped(graphDir.toString(), pl);

        pl.logger.info("Symmetrizing");
        graph = Transform.symmetrize(graph, pl);

        pl.logger.info("Removing loops");
        graph =  Transform.filterArcs(graph, Transform.NO_LOOPS, pl);

        pl.logger.info("Permuting");
        LayeredLabelPropagation llp = new LayeredLabelPropagation(graph, SEED);
        int[] permutation = llp.computePermutation(clusterFile.toString());

        pl.logger.info("Applying permutation");
        graph = Transform.map(graph, permutation, pl);

        pl.logger.info("Storing graph");
        BVGraph.store(graph, compressedGraphDir.toString(), pl);
    }
}
