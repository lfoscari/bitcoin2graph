package it.unimi.dsi.law;


import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.big.webgraph.examples.IntegerListImmutableGraph;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.RocksDBWrapper.Column.INPUT;

public class BlockchainGraph extends ImmutableSequentialGraph {
    private final long numNodes;

    public BlockchainGraph() {
        try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(addresses.toFile()))) {
            lineNumberReader.skip(Integer.MAX_VALUE);
            this.numNodes = lineNumberReader.getLineNumber() + 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long numNodes() {
        return this.numNodes;
    }

    @Override
    public NodeIterator nodeIterator() {
        try {
            return new NodeIterator() {
                final RocksDBWrapper database = new RocksDBWrapper(true, transactionsDatabaseDirectory);
                final RocksIterator addressesIterator = this.database.iterator(INPUT);
                final ByteBuffer currentAddress = ByteBuffer.allocateDirect(Long.BYTES);

                {
                    this.addressesIterator.seekToFirst();
                    this.addressesIterator.key(this.currentAddress);
                    this.currentAddress.clear();
                }

                long[] outputAddresses = Utils.bytesToLongs(this.addressesIterator.value());
                long addressCount = -1;

                long[][] successorsArray = LongBigArrays.EMPTY_BIG_ARRAY;

                @Override
                public long outdegree() {
                    return this.outputAddresses.length;
                }

                @Override
                public long nextLong() {
                    if (!this.hasNext()) {
                        throw new NoSuchElementException();
                    }

                    this.addressesIterator.next();

                    if (!this.addressesIterator.isValid()) {
                        throw new NoSuchElementException();
                    }

                    this.addressesIterator.key(this.currentAddress);
                    this.currentAddress.clear();

                    this.outputAddresses = Utils.bytesToLongs(this.addressesIterator.value());
                    this.addressCount++;

                    return this.currentAddress.getLong();
                }

                @Override
                public boolean hasNext() {
                    return this.addressCount < BlockchainGraph.this.numNodes - 1;
                }

                @Override
                public long[][] successorBigArray() {
                    if (this.addressCount == -1) {
                        throw new IllegalStateException();
                    }

                    this.successorsArray = BigArrays.ensureCapacity(this.successorsArray, this.outputAddresses.length, 0);

                    for (int i = 0; i < this.outputAddresses.length; i++) {
                        BigArrays.set(this.successorsArray, i, this.outputAddresses[i]);
                    }

                    return this.successorsArray;
                }

                @SuppressWarnings("deprecation")
                @Override
                protected void finalize() throws Throwable {
                    try {
                        this.database.close();
                    } finally {
                        super.finalize();
                    }
                }

                @Override
                public NodeIterator copy(final long upperBound) {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static ImmutableGraph load(final CharSequence ignoredBasename, final ProgressLogger ignoredPl) {
        throw new UnsupportedOperationException("Graphs may be loaded offline only");
    }

    public static ImmutableGraph load(final CharSequence basename) {
        return load(basename, null);
    }

    @Deprecated
    public static ImmutableGraph loadSequential(final CharSequence basename, final ProgressLogger pl) {
        return load(basename, pl);
    }

    @Deprecated
    public static ImmutableGraph loadSequential(final CharSequence basename) {
        return load(basename, null);
    }

    public static ImmutableGraph loadOffline(final CharSequence ignoredBasename, final ProgressLogger ignoredPl) {
        return new BlockchainGraph();
    }

    public static ImmutableGraph loadOffline(final CharSequence basename) {
        return loadOffline(basename, null);
    }

    public static void main(String[] args) throws IOException {
        ImmutableGraph graph = new BlockchainGraph();
        BVGraph.store(graph, basename.toString());
    }
}
