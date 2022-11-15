package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.TransactionAddresses;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.CustomBlockchainIterator.outputAddresses;
import static it.unimi.dsi.law.Parameters.COINBASE_ADDRESS;

public class PopulateMappings implements Runnable {
	private final List<byte[]> blocksBytes;
	public final LinkedBlockingQueue<Long[]> transactionArcs;
	private final LinkedBlockingQueue<WriteBatch> wbQueue;
	private final AddressConversion addressConversion;

	private final ColumnFamilyHandle incompleteMappings;
	private final ColumnFamilyHandle topFilter;
	private final ColumnFamilyHandle transactionAddresses;

	private final NetworkParameters np;
	private final ProgressLogger progress;

	private final WriteBatch wb;

	public PopulateMappings (List<byte[]> blocksBytes, AddressConversion addressConversion, LinkedBlockingQueue<Long[]> transactionArcs, List<ColumnFamilyHandle> columnFamilyHandleList, LinkedBlockingQueue<WriteBatch> wbQueue, NetworkParameters np, ProgressLogger progress) {
		this.blocksBytes = blocksBytes;
		this.transactionArcs = transactionArcs;
		this.wbQueue = wbQueue;
		this.addressConversion = addressConversion;

		this.incompleteMappings = columnFamilyHandleList.get(1);
		this.topFilter = columnFamilyHandleList.get(2);
		this.transactionAddresses = columnFamilyHandleList.get(3);

		this.np = np;
		this.progress = progress;

		this.wb = new WriteBatch();
	}

	private void populateMappings () throws RocksDBException, InterruptedException {
		for (byte[] blockBytes : this.blocksBytes) {
			Block block = this.np.getDefaultSerializer().makeBlock(blockBytes);

            if (!block.hasTransactions()) {
                return;
            }

			List<Transaction> transactions = (List<Transaction>) Blockchain2ScatteredArcsASCIIGraph.extract(block, "transactions");

			for (Transaction transaction : transactions) {
				List<byte[]> outputs = outputAddresses(transaction, this.np);

				if (transaction.isCoinBase()) {
					this.addCoinbaseArcs(outputs);
				} else {
					this.storeTransaction(transaction, outputs);
				}
			}

			this.progress.update();
		}
	}

	public void storeTransaction (Transaction transaction, List<byte[]> receivers) throws RocksDBException {
		for (TransactionInput ti : transaction.getInputs()) {
			TransactionOutPoint top = ti.getOutpoint();

			IncompleteMappings.put(this.wb, this.incompleteMappings, top.hashCode(), receivers, transaction.getUpdateTime());
			TransactionOutpointFilter.put(this.wb, this.topFilter, top.getHash(), top.getIndex(), transaction.getUpdateTime());
		}

		TransactionAddresses.put(this.wb, this.transactionAddresses, transaction.getTxId(), receivers, transaction.getUpdateTime());
	}

	public void addCoinbaseArcs (List<byte[]> receivers) throws InterruptedException, RocksDBException {
		for (byte[] receiver : receivers) {
			long r = addressConversion.map(LegacyAddress.fromPubKeyHash(np, receiver));
			this.transactionArcs.put(new Long[] { COINBASE_ADDRESS, r });
		}
	}

	@Override
	public void run () {
		try {
			this.populateMappings();
			this.wbQueue.put(this.wb);
		} catch (RocksDBException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
