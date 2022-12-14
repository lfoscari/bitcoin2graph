package it.unimi.dsi.law;

import org.rocksdb.*;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static it.unimi.dsi.law.Parameters.*;

public class RocksDBWrapper implements Closeable {
    private Options options;
    private final RocksDB database;
    private final boolean readonly;

    private final WriteBatch writeBatch;
    private final long WB_LIMIT = 10_000;

    public RocksDBWrapper(boolean readonly, Path location) throws RocksDBException {
        RocksDB.loadLibrary();
        this.readonly = readonly;

        this.options = new Options().setCreateIfMissing(true)
                .setDbWriteBufferSize(WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(MAX_BACKGROUND_JOBS)
                .setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE);

        if (this.readonly) {
            this.writeBatch = null;
            this.database = RocksDB.openReadOnly(this.options, location.toString());
        } else {
            this.options = this.options
                    .setMergeOperator(new StringAppendOperator());

            this.writeBatch = new WriteBatch();
            this.database = RocksDB.open(this.options, location.toString());
        }
    }

    public void add(byte[] transaction, byte[] address) throws RocksDBException {
        this.writeBatch.merge(transaction, address);
        if (this.writeBatch.getDataSize() > this.WB_LIMIT) {
            this.commit();
        }
    }

    public byte[] get(byte[] transaction) throws RocksDBException {
        return this.database.get(transaction);
    }

    public RocksIterator iterator() {
        return this.database.newIterator();
    }

    private void commit() throws RocksDBException {
        this.database.write(new WriteOptions(), this.writeBatch);
        this.writeBatch.clear();
    }

    public void close() {
        if (!this.readonly) {
            try {
                this.commit();
                this.database.syncWal();
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        this.database.close();
        this.options.close();
    }
}
