package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.law.utils.ByteConversion;
import it.unimi.dsi.logging.ProgressLogger;
import org.apache.commons.math3.analysis.function.Add;
import org.bitcoinj.core.*;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static it.unimi.dsi.law.CustomBlockchainIterator.transactionOutputToAddress;
import static org.bitcoinj.script.Script.ScriptType.P2WPKH;

/**
 * Build an address conversion map from either a tsv file
 * in the Blockchair format or by scanning the blockchain
 * files.
 */
public class AddressConversion {
    private final NetworkParameters np;
    private ProgressLogger progress;

    private long count = 0;
    private Path location;

    private Options options;
    private RocksDB db;

    public AddressConversion() throws RocksDBException, IOException {
        this(null);
    }

    public AddressConversion(ProgressLogger progress) throws RocksDBException, IOException {
        if (progress == null) {
            Logger l = LoggerFactory.getLogger(getClass());
            progress = new ProgressLogger(l, Parameters.logInterval, Parameters.logTimeUnit, "blocks");
        }

        this.progress = progress;
        this.np = new MainNetParams();
        this.db = startDatabase();

        new Context(np);
    }

    private RocksDB startDatabase() throws RocksDBException, IOException {
        RocksDB.loadLibrary();

        this.location = Path.of(Parameters.resources + "addressconversion");
        boolean created = this.location.toFile().mkdir();

        if (!created) {
            throw new RuntimeException("Address conversion database already present!");
        }

        options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbWriteBufferSize(Parameters.WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(Parameters.MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(Parameters.MAX_BACKGROUND_JOBS);

        return RocksDB.open(options, location.toString());
    }

    public void addAddresses(File tsv) throws IOException, RocksDBException {
        progress.start("Adding addresses from " + tsv.toString());

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
                throw new RuntimeException("Malformed tsv");

            progress.update();

            byte[] address = Arrays.copyOfRange(line, 0, read);
            Address converted;

            try {
                converted = LegacyAddress.fromString(np, new String(address));
                this.db.put(converted.getHash(), ByteConversion.long2bytes(count++));
            } catch (AddressFormatException e) {
                System.err.println("Cannot cast address " + e.getMessage());
            }
        }

        // Save current count
        this.db.put("count".getBytes(), ByteConversion.long2bytes(count));

        progress.stop();
    }

    public void addAddresses(List<File> blockfiles) throws RocksDBException {
        progress.start("Adding addresses from " + blockfiles.size() + " blockfiles");
        BlockFileLoader bfl = new BlockFileLoader(np, blockfiles);

        for (Block block : bfl) {
            if (!block.hasTransactions())
                continue;

            progress.update();

            try (WriteBatch wb = new WriteBatch()) {
                for (Transaction transaction : block.getTransactions()) {
                    for (TransactionOutput to : transaction.getOutputs()) {
                        Address receiver = transactionOutputToAddress(to, np);

                        if (receiver == null)
                            continue;

                        // I'm using receiver.getHash() because it would lose information
                        wb.put(receiver.toString().getBytes(), ByteConversion.long2bytes(count++));
                    }
                }

                this.db.write(new WriteOptions(), wb);
            }
        }

        // Save current count
        this.db.put("count".getBytes(), ByteConversion.long2bytes(count));

        progress.done();
    }

    /**
     * Opens the address conversion database as readonly, to improve multithread
     * performance
     */
    public void freeze() throws RocksDBException {
        this.db.close();
        this.db = RocksDB.openReadOnly(this.options, this.location.toString());
    }

    public long map(Address address) throws RocksDBException {
        return ByteConversion.bytes2long(this.db.get(address.toString().getBytes()));
    }
}
