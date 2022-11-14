package it.unimi.dsi.law;

import it.unimi.dsi.law.utils.ByteConversion;
import org.rocksdb.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class RocksArcs implements Runnable {
    private final LinkedBlockingQueue<Long[]> transactionArcs;
    public volatile  boolean stop = false;

    private final RocksDB db;
    private Options options;

    public RocksArcs(LinkedBlockingQueue<Long[]> transactionArcs) throws RocksDBException {
        this.transactionArcs = transactionArcs;
        this.db = startDatabase();
    }

    public void flush() {
        try (WriteBatch wb = new WriteBatch()) {
            List<Long[]> arcsList = new ArrayList<>();
            transactionArcs.drainTo(arcsList);

            for (Long[] arcs: arcsList) {
                Long sender = arcs[0], receiver = arcs[1];
                wb.merge(ByteConversion.long2bytes(sender), ByteConversion.long2bytes(receiver));
            }

            this.db.write(new WriteOptions(), wb);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (true) {
            if (stop) {
                this.close();
                break;
            }

            if (transactionArcs.size() < 1000)
                continue;

           flush();
        }
    }

    private void close() {
        this.options.close();
        this.db.close();
    }

    private RocksDB startDatabase() throws RocksDBException {
        RocksDB.loadLibrary();

        Path location = Path.of(Parameters.resources + "arcs");
        location.toFile().mkdir();

        options = new Options()
                .setCreateIfMissing(true)
                .setDbWriteBufferSize(Parameters.WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(Parameters.MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(Parameters.MAX_BACKGROUND_JOBS)
                .setMergeOperator(new StringAppendOperator());

        return RocksDB.open(options, location.toString());
    }
}
