package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.law.persistence.AddressConversion;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import static it.unimi.dsi.law.CustomBlockchainIterator.*;

public class PopulateMappings implements Callable<PersistenceLayer> {
    private final NetworkParameters np;
    private final ProgressLogger progress;
    private final List<File> blockFiles;

    public final LongArrayFIFOQueue transactionArcs = new LongArrayFIFOQueue();

    private final PersistenceLayer persistenceLayer;
    private final AddressConversion addressConversion;
    private final IncompleteMappings incompleteMappings;
    private final TransactionOutpointFilter topFilter;

    public PopulateMappings(List<File> blockFiles, NetworkParameters np, ProgressLogger progress) throws IOException, RocksDBException {
        this.np = np;
        this.progress = progress;
        this.blockFiles = blockFiles;

        Path tempDirectory = Files.createTempDirectory(Path.of(Parameters.resources), "blocks-");

        this.persistenceLayer = new PersistenceLayer(tempDirectory.toString());
        this.addressConversion = persistenceLayer.getAddressConversion();
        this.incompleteMappings = persistenceLayer.getIncompleteMappings();
        this.topFilter = persistenceLayer.getTransactionOutpointFilter();
    }

    void populateMappings() throws RocksDBException {
        BlockFileLoader bflTemp = new BlockFileLoader(np, blockFiles);

        for (Block block : bflTemp) {
            progress.update();

            if (!block.hasTransactions())
                continue;

            for (Transaction transaction : block.getTransactions()) {
                List<Long> outputs = outputAddressesToLongs(transaction, this.addressConversion, this.np);

                if (transaction.isCoinBase()) {
                    addCoinbaseArcs(outputs, transactionArcs);
                } else {
                    mapTransactionOutputs(transaction, outputs, this.incompleteMappings, this.topFilter);
                }
            }
        }
    }

    @Override
    public PersistenceLayer call() {
        try {
            populateMappings();
            return persistenceLayer;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
