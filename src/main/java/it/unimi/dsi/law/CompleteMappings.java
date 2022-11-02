package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.CustomBlockchainIterator.outputAddressesToLongs;

public class CompleteMappings implements Runnable {
    private final NetworkParameters np;
    private final ProgressLogger progress;
    private final List<File> blockFiles;
    private final AddressConversion addressConversion;

    private final LinkedBlockingQueue<Long> transactionArcs;

    private final TransactionOutpointFilter topFilter;
    private final IncompleteMappings incompleteMappings;

    public CompleteMappings(List<File> blockFiles, LinkedBlockingQueue<Long> transactionArcs, PersistenceLayer persistenceLayer, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) {
        this.np = np;
        this.progress = progress;
        this.blockFiles = blockFiles;

        this.transactionArcs = transactionArcs;
        this.addressConversion = addressConversion;

        this.topFilter = persistenceLayer.getTransactionOutpointFilter();
        this.incompleteMappings = persistenceLayer.getIncompleteMappings();
    }

    @Override
    public void run() {
        try {
            this.completeMappings();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void completeMappings() throws RocksDBException {
        BlockFileLoader bfl = new BlockFileLoader(np, this.blockFiles);

        for (Block block : bfl) {
            progress.update();

            if (!block.hasTransactions())
                continue;

            for (Transaction transaction : block.getTransactions()) {
                List<Long> senders = outputAddressesToLongs(transaction, addressConversion, np);

                Sha256Hash txId = transaction.getTxId();
                List<Long> indices = topFilter.get(txId);

                if (indices.size() == 0)
                    continue;

                for (long index : indices) {
                    TransactionOutPoint top = new TransactionOutPoint(np, index, txId);
                    long sender = senders.get((int) index);

                    for (Long receiver : incompleteMappings.get(top)) {
                        if (receiver == null)
                            continue;

                        transactionArcs.add(sender);
                        transactionArcs.add(receiver);
                    }
                }
            }
        }
    }
}
