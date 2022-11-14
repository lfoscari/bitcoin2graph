package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.law.utils.ByteConversion;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.*;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static it.unimi.dsi.law.CustomBlockchainIterator.transactionOutputToAddress;

/**
 * Build an address conversion map from either a tsv file
 * in the Blockchair format or by scanning the blockchain
 * files.
 */
public class AddressConversion implements Closeable {
    private final NetworkParameters np;
    private final ProgressLogger progress;

    private long count = 0;

    private Options options;
    private final RocksDB db;

    public AddressConversion(NetworkParameters np) throws RocksDBException, IOException {
        this(np, null, false);
    }

    public AddressConversion(NetworkParameters np, boolean readonly) throws RocksDBException, IOException {
        this(np, null, readonly);
    }

    public AddressConversion(NetworkParameters np, ProgressLogger progress) throws RocksDBException, IOException {
        this(np, progress, false);
    }

    public AddressConversion(NetworkParameters np, ProgressLogger progress, boolean readonly) throws RocksDBException, IOException {
        if (progress == null) {
            Logger l = LoggerFactory.getLogger(this.getClass());
            progress = new ProgressLogger(l, Parameters.logInterval, Parameters.logTimeUnit, "blocks");
        }
        
        this.np = np;
        this.progress = progress;
        this.db = this.startDatabase(readonly);
    }

    private RocksDB startDatabase (boolean readonly) throws RocksDBException {
        RocksDB.loadLibrary();

        Path location = Path.of(Parameters.resources + "addressconversion");
        location.toFile().mkdir();

        this.options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbWriteBufferSize(Parameters.WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(Parameters.MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(Parameters.MAX_BACKGROUND_JOBS);

        if (readonly) {
            return RocksDB.openReadOnly(this.options, location.toString());
        }

        return RocksDB.open(this.options, location.toString());
    }

    public void addAddresses(File tsv) throws IOException, RocksDBException {
        this.progress.start("Adding addresses from " + tsv.toString());

        byte tab = 9;
        byte[] line = new byte[80]; // max length

        FastBufferedInputStream is = new FastBufferedInputStream(new FileInputStream(tsv));
        is.readLine(line); // Skip header

        while (is.available() > 0) {
            int read = is.readLine(line);

            // Tab position
            while (read > 0 && line[read] != tab)
                read--;

            if (read == 0)
                throw new RuntimeException("Malformed tsv! The right format is [address]<tab>...<newline>");

            this.progress.update();

            byte[] address = Arrays.copyOfRange(line, 0, read);
            Address converted;

            try {
                converted = LegacyAddress.fromString(this.np, new String(address));
                this.db.put(converted.getHash(), ByteConversion.long2bytes(this.count++));
            } catch (AddressFormatException e) {
                System.err.println("Cannot cast address " + e.getMessage());
            }
        }

        this.progress.stop();
    }

    public void addAddresses(List<File> blockFiles) throws RocksDBException {
        this.progress.start("Adding addresses from " + blockFiles.size() + " blockfiles");
        BlockFileLoader bfl = new BlockFileLoader(this.np, blockFiles);

        for (Block block : bfl) {
            if (!block.hasTransactions())
                continue;

            this.progress.update();

            try (WriteBatch wb = new WriteBatch()) {
                for (Transaction transaction : block.getTransactions()) {
                    for (TransactionOutput to : transaction.getOutputs()) {
                        Address receiver = transactionOutputToAddress(to, this.np);

                        if (receiver == null)
                            continue;

                        // I'm not using receiver.getHash() because it would lose information
                        byte[] key = receiver.toString().getBytes();
                        wb.put(key, ByteConversion.long2bytes(this.count++));
                    }
                }

                this.db.write(new WriteOptions(), wb);
            }
        }

        this.progress.done();
    }

    public void close() {
        this.options.close();
        this.db.close();
    }

    public long map(Address address) throws RocksDBException {
        byte[] key = address.toString().getBytes();
        byte[] value = this.db.get(key);

        return ByteConversion.bytes2long(value);
    }

    public RocksIterator iterator() {
        RocksIterator rit = this.db.newIterator();
        rit.seekToFirst();

        return rit;
    }
}
