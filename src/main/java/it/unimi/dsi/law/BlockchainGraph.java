package it.unimi.dsi.law;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;

import java.io.InputStream;

public class BlockchainGraph extends ImmutableGraph {
    @Override
    public int numNodes() {
        return 0;
    }

    @Override
    public boolean randomAccess() {
        return false;
    }

    @Override
    public int outdegree(int i) {
        return 0;
    }

    @Override
    public ImmutableGraph copy() {
        return null;
    }

    public static ImmutableGraph load(CharSequence basename, ProgressLogger progress) {
        return null;
    }

    public static ImmutableGraph load(CharSequence basename) {
        return null;
    }

    public static ImmutableGraph loadOffline(CharSequence basename, ProgressLogger progress) {
        return null;
    }

    public static ImmutableGraph loadOffline(CharSequence basename) {
        return null;
    }

    public static ImmutableGraph loadOnce(InputStream is) {
        return null;
    }

    public static void store(ImmutableGraph graph, CharSequence basename, ProgressLogger progress) {

    }

    public static void store(ImmutableGraph graph, CharSequence basename) {

    }
}
