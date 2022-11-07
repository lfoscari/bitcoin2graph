package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.CustomBlockchainIterator.outputAddressesToLongs;

public class CompleteMappings implements Runnable {
    private final List<byte[]> blocksBytes;
    public final LinkedBlockingQueue<Long> transactionArcs;
    private final PersistenceLayer mappings;
    private final AddressConversion addressConversion;

    private final NetworkParameters np;
    private final ProgressLogger progress;

    public CompleteMappings(List<byte[]> blocksBytes, AddressConversion addressConversion, LinkedBlockingQueue<Long> transactionArcs, PersistenceLayer mappings, NetworkParameters np, ProgressLogger progress) {
        this.blocksBytes = blocksBytes;
        this.addressConversion = addressConversion;
        this.transactionArcs = transactionArcs;
        this.mappings = mappings;

        this.np = np;
        this.progress = progress;
    }

    public void completeMappings() throws RocksDBException {
        for (byte[] blockBytes : blocksBytes) {
            Block block = np.getDefaultSerializer().makeBlock(blockBytes);

            if (!block.hasTransactions())
                return;

            for (Transaction transaction : block.getTransactions()) {
                List<Long> senders = outputAddressesToLongs(transaction, addressConversion, np);

                Sha256Hash txId = transaction.getTxId();
                List<Long> indices = TransactionOutpointFilter.get(mappings, txId, transaction.getUpdateTime());

                if (indices.size() == 0)
                    continue;

                for (long index : indices) {
                    TransactionOutPoint top = new TransactionOutPoint(np, index, txId);
                    long sender = senders.get((int) index);

                    for (Long receiver : IncompleteMappings.get(mappings, top, transaction.getUpdateTime())) {
                        if (receiver == null)
                            continue;

                        transactionArcs.add(sender);
                        transactionArcs.add(receiver);
                    }
                }
            }

            this.progress.update();
        }
    }

    @Override
    public void run () {
        try {
            this.completeMappings();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
