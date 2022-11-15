package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.CustomBlockchainIterator.outputAddressesToLongs;

public class CompleteMappings implements Runnable {
	private final List<byte[]> blocksBytes;
	public final LinkedBlockingQueue<Long[]> transactionArcs;
	private final PersistenceLayer mappings;
	private final AddressConversion addressConversion;

	private final NetworkParameters np;
	private final ProgressLogger progress;

	public CompleteMappings (List<byte[]> blocksBytes, AddressConversion addressConversion, LinkedBlockingQueue<Long[]> transactionArcs, PersistenceLayer mappings, NetworkParameters np, ProgressLogger progress) {
		this.blocksBytes = blocksBytes;
		this.addressConversion = addressConversion;
		this.transactionArcs = transactionArcs;
		this.mappings = mappings;

		this.np = np;
		this.progress = progress;
	}

	public void completeMappings () throws RocksDBException, InterruptedException {
		for (byte[] blockBytes : this.blocksBytes) {
			Block block = this.np.getDefaultSerializer().makeBlock(blockBytes);

            if (!block.hasTransactions()) {
                return;
            }

			List<Transaction> transactions = (List<Transaction>) Blockchain2ScatteredArcsASCIIGraph.extract(block, "transactions");

			for (Transaction transaction : transactions) {
				this.transactionToArcs(transaction);
			}

			this.progress.update();
		}
	}

	private void transactionToArcs (Transaction transaction) throws RocksDBException {
		List<Long> senders = outputAddressesToLongs(transaction, this.addressConversion, this.np);

		Sha256Hash txId = transaction.getTxId();
		List<Long> indices = TransactionOutpointFilter.get(this.mappings, txId, transaction.getUpdateTime());

		for (Long index : indices) {
			Long sender = senders.get(index.intValue());
			this.extractArcs(transaction, sender, txId, index);
		}
	}

	private void extractArcs (Transaction transaction, Long sender, Sha256Hash txId, Long index) throws RocksDBException {
		int topHashCode = Objects.hash(index, txId);

		for (Long receiver : IncompleteMappings.get(this.mappings, topHashCode, transaction.getUpdateTime())) {
			if (receiver == -1) {
				continue;
			}

			this.transactionArcs.add(new Long[] { sender, receiver });
		}
	}

	@Override
	public void run () {
		try {
			this.completeMappings();
		} catch (RocksDBException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
