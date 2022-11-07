package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static it.unimi.dsi.law.CustomBlockchainIterator.outputAddressesToLongs;
import static it.unimi.dsi.law.Parameters.COINBASE_ADDRESS;

public class PopulateMappings implements Callable<WriteBatch> {
    private final List<byte[]> blocksBytes;
    public final LinkedBlockingQueue<Long> transactionArcs;
    private final AddressConversion addressConversion;

    private final ColumnFamilyHandle incompleteMappings;
    private final ColumnFamilyHandle topFilter;

    private final NetworkParameters np;
    private final ProgressLogger progress;

    private final WriteBatch wb;

    public PopulateMappings(List<byte[]> blocksBytes, AddressConversion addressConversion, LinkedBlockingQueue<Long> transactionArcs, List<ColumnFamilyHandle> columnFamilyHandleList, NetworkParameters np, ProgressLogger progress) {
        this.blocksBytes = blocksBytes;
        this.transactionArcs = transactionArcs;
        this.addressConversion = addressConversion;

        this.incompleteMappings = columnFamilyHandleList.get(1);
        this.topFilter = columnFamilyHandleList.get(2);

        this.np = np;
        this.progress = progress;

        this.wb = new WriteBatch();
    }

    private void populateMappings() throws RocksDBException {
        for (byte[] blockBytes : blocksBytes) {
            Block block = np.getDefaultSerializer().makeBlock(blockBytes);

            if (!block.hasTransactions())
                return;

            for (Transaction transaction : block.getTransactions()) {
                List<Long> outputs = outputAddressesToLongs(transaction, this.addressConversion, this.np);

                if (transaction.isCoinBase()) {
                    addCoinbaseArcs(outputs);
                } else {
                    mapTransactionOutputs(transaction, outputs);
                }
            }

            this.progress.update();
        }
    }

    public void mapTransactionOutputs(Transaction transaction, List<Long> receivers) throws RocksDBException {
        for (TransactionInput ti : transaction.getInputs()) {
            TransactionOutPoint top = ti.getOutpoint();

            IncompleteMappings.put(wb, incompleteMappings, top, receivers, transaction.getUpdateTime());
            TransactionOutpointFilter.put(wb, topFilter, top.getHash(), top.getIndex(), transaction.getUpdateTime());
        }
    }

    public void addCoinbaseArcs(List<Long> receivers) {
        for (long receiver : receivers) {
            transactionArcs.add(COINBASE_ADDRESS);
            transactionArcs.add(receiver);
        }
    }

    @Override
    public WriteBatch call() {
        try {
            this.populateMappings();
            return wb;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
