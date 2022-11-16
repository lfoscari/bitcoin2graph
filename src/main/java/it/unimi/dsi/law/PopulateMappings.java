package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.Arcs;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.TransactionAddresses;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptException;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.Parameters.*;

public class PopulateMappings implements Runnable {
	private final List<byte[]> blocksBytes;
	private final LinkedBlockingQueue<WriteBatch> wbQueue;

	private final ColumnFamilyHandle incompleteMappings;
	private final ColumnFamilyHandle transactionAddresses;
	private final ColumnFamilyHandle arcs;

	private final NetworkParameters np;
	private final ProgressLogger progress;

	private WriteBatch wb;

	public PopulateMappings (List<byte[]> blocksBytes, List<ColumnFamilyHandle> columnFamilyHandleList, LinkedBlockingQueue<WriteBatch> wbQueue, NetworkParameters np, ProgressLogger progress) {
		this.blocksBytes = blocksBytes;
		this.wbQueue = wbQueue;

		this.incompleteMappings = columnFamilyHandleList.get(1);
		this.transactionAddresses = columnFamilyHandleList.get(2);
		this.arcs = columnFamilyHandleList.get(3);

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

			for (Transaction transaction : block.getTransactions()) {
				this.storeTransaction(transaction);
			}

			this.commit();
			this.progress.update();
		}
	}

	public void storeTransaction (Transaction transaction) throws RocksDBException {
		if (transaction.isCoinBase()) {
			// Do we really need these???
			// this.addCoinbaseArcs(outputs);
			return;
		}

		List<byte[]> outputs = this.outputAddresses(transaction);

		Sha256Hash txId = transaction.getTxId();
		TransactionAddresses.put(this.wb, this.transactionAddresses, txId, outputs);

		for (TransactionInput ti : transaction.getInputs()) {
			TransactionOutPoint top = ti.getOutpoint();
			IncompleteMappings.put(this.wb, this.incompleteMappings, txId, top);
		}
	}

	public void addCoinbaseArcs (List<byte[]> receivers) throws RocksDBException {
		for (byte[] receiver : receivers) {
			Arcs.put(this.wb, this.arcs, COINBASE_ADDRESS, receiver);
		}
	}

	public List<byte[]> outputAddresses (Transaction t) {
		return t.getOutputs().stream().map(this::transactionOutputToAddress).toList();
	}

	public byte[] transactionOutputToAddress (TransactionOutput to) {
		try {
			Script script = to.getScriptPubKey();

			if (script.getScriptType() == null) {
				// No public keys are contained in this script.
				return MISSING_ADDRESS;
			}

			return script.getToAddress(this.np, true).getHash();
		} catch (IllegalArgumentException | ScriptException e) {
			// Non-standard address
			return UNKNOWN_ADDRESS;
		}
	}

	public void commit() {
		if (this.wb.getDataSize() > Parameters.WRITE_BUFFER_SIZE) {
			this.wbQueue.add(this.wb);
			this.wb = new WriteBatch();
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
