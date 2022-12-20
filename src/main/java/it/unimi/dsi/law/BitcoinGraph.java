package it.unimi.dsi.law;

import it.unimi.dsi.webgraph.*;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.RocksDBWrapper.Column.INPUT;
import static it.unimi.dsi.law.RocksDBWrapper.Column.OUTPUT;

public class BitcoinGraph extends ImmutableSequentialGraph {
    private final RocksDBWrapper database;
    private final ProgressLogger progress;

    private BitcoinGraph(final Path databaseLocation) throws RocksDBException {
        this.database = new RocksDBWrapper(true, databaseLocation);

        Logger logger = LoggerFactory.getLogger(this.getClass());
        this.progress = new ProgressLogger(logger, "nodes");
        this.progress.displayLocalSpeed = true;
        this.progress.expectedUpdates = this.numNodes();
    }

    @Override
    public int numNodes() {
        return 12697;
    }

    @Override
    public boolean randomAccess() {
        return false;
    }

    @Override
    public NodeIterator nodeIterator() {
        RocksDBWrapper databaseBorrow = this.database;
        ProgressLogger progressBorrow = this.progress;

        return new NodeIterator() {
            final RocksDBWrapper database = databaseBorrow;
            final RocksIterator inputIterator = this.database.iterator(INPUT);
            final RocksIterator outputIterator = this.database.iterator(OUTPUT);

            private final ByteBuffer inputAddress = ByteBuffer.allocate(Integer.BYTES);
            private final ByteBuffer outputAddress = ByteBuffer.allocate(Integer.BYTES);

            private int[] successors;

            {
                this.inputIterator.seekToFirst();
                this.outputIterator.seekToFirst();
            }

            @Override
            public int outdegree() {
                if (this.successors == null) {
                    throw new IllegalStateException();
                }

                return this.successors.length;
            }

            @Override
            public int nextInt() {
                for (; this.outputIterator.isValid(); this.outputIterator.next()) {
                    this.outputIterator.key(this.outputAddress);

                    for (; this.inputIterator.isValid(); this.inputIterator.next()) {
                        this.inputIterator.key(this.inputAddress);
                        int cmp = Arrays.compareUnsigned(this.outputAddress.array(), this.inputAddress.array());

                        if (cmp == 0) {
                            progressBorrow.lightUpdate();

                            this.successors = Utils.distinct(Utils.intersect(
                                    Utils.bytesToInts(this.outputIterator.value()),
                                    Utils.bytesToInts(this.inputIterator.value())
                            ));

                            this.outputIterator.next();
                            this.inputIterator.next();

                            return Utils.bytesToInts(this.outputAddress.array())[0];
                        } else if (cmp < 0) {
                            // Address is missing
                            break;
                        }
                    }
                }

                this.close();
                throw new NoSuchElementException();
            }

            @Override
            public int[] successorArray() {
                if (this.successors == null) {
                    throw new IllegalStateException();
                }

                return this.successors;
            }

            @Override
            public boolean hasNext() {
                return this.outputIterator.isValid();
            }

            private void close () {
                this.inputIterator.close();
                this.outputIterator.close();
                this.database.close();
            }
        };
    }

    public static void main(String[] args) throws IOException, RocksDBException {
        ImmutableSequentialGraph bg = new BitcoinGraph(addressDatabaseDirectory);
        EFGraph.store(bg, basename.toString());
    }
}
