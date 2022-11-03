package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.CustomBlockchainIterator.outputAddressesToLongs;
import static it.unimi.dsi.law.Parameters.COINBASE_ADDRESS;

public class PopulateMappings implements Callable<PersistenceLayer> {
    private final NetworkParameters np;
    private final ProgressLogger progress;
    private final List<File> blockFiles;
    private final AddressConversion addressConversion;

    public final Path tempDirectory;
    public final LinkedBlockingQueue<Long> transactionArcs;

    private final PersistenceLayer persistenceLayer;
    private final IncompleteMappings incompleteMappings;
    private final TransactionOutpointFilter topFilter;

    public PopulateMappings(List<File> blockFiles, LinkedBlockingQueue<Long> transactionArcs, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) throws IOException, RocksDBException {
        this.np = np;
        this.progress = progress;
        this.blockFiles = blockFiles;
        this.transactionArcs = transactionArcs;
        this.addressConversion = addressConversion;

        tempDirectory = Files.createTempDirectory(Path.of(Parameters.resources), "partialchain-");

        this.persistenceLayer = new PersistenceLayer(tempDirectory.toString());
        this.incompleteMappings = persistenceLayer.getIncompleteMappings();
        this.topFilter = persistenceLayer.getTransactionOutpointFilter();
    }

    void populateMappings() throws RocksDBException {
        BlockFileLoader bfl = new BlockFileLoader(np, blockFiles);

        for (Block block : bfl) {
            progress.update();

            if (!block.hasTransactions())
                continue;

            for (Transaction transaction : block.getTransactions()) {
                List<Long> outputs = outputAddressesToLongs(transaction, this.addressConversion, this.np);

                if (transaction.isCoinBase()) {
                    addCoinbaseArcs(outputs);
                } else {
                    mapTransactionOutputs(transaction, outputs);
                }
            }
        }
    }

    public void mapTransactionOutputs(Transaction transaction, List<Long> receivers) throws RocksDBException {
        for (TransactionInput ti : transaction.getInputs()) {
            TransactionOutPoint top = ti.getOutpoint();

            incompleteMappings.put(top, receivers);
            topFilter.put(top.getHash(), top.getIndex());
        }
    }

    public void addCoinbaseArcs(List<Long> receivers) {
        for (long receiver : receivers) {
            transactionArcs.add(COINBASE_ADDRESS);
            transactionArcs.add(receiver);
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
