package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.Arcs;
import it.unimi.dsi.law.persistence.TransactionAddresses;
import it.unimi.dsi.law.utils.ByteConversion;
import it.unimi.dsi.logging.ProgressLogger;
import org.apache.commons.math3.geometry.spherical.oned.Arc;
import org.bitcoinj.core.*;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class CompleteMappings {
	private final PersistenceLayer mappings;
	private final ColumnFamilyHandle incompleteMappings;
	private final ColumnFamilyHandle arcs;

	private final LinkedBlockingQueue<WriteBatch> wbQueue;
	private WriteBatch wb;

	private final ProgressLogger progress;

	public CompleteMappings (PersistenceLayer mappings, LinkedBlockingQueue<WriteBatch> wbQueue, ProgressLogger progress) throws RocksDBException {
		this.mappings = mappings;

		List<ColumnFamilyHandle> columnFamilyHandleList = mappings.getColumnFamilyHandleList();
		this.incompleteMappings = columnFamilyHandleList.get(1);
		this.arcs = columnFamilyHandleList.get(3);

		this.wbQueue = wbQueue;
		this.wb = new WriteBatch();

		this.progress = progress;

		this.completeMappings();
	}

	public void completeMappings () throws RocksDBException {
		for (RocksIterator it = this.mappings.iterator(this.incompleteMappings); it.isValid(); it.next()) {
			Sha256Hash txId = Sha256Hash.wrap(it.key());
			List<Pair<Sha256Hash, Long>> incomplete = IncompleteMappings.parse(it.value());

			if (incomplete == null) {
				continue;
			}

			List<byte[]> receivers = TransactionAddresses.get(this.mappings, txId);

			if (receivers == null) {
				continue;
			}

			// Remove duplicates
			List<byte[]> uniqueReceivers = receivers.stream().distinct().toList();

			for (Pair<Sha256Hash, Long> top : incomplete) {
				this.findArcs(top.left(), top.right(), uniqueReceivers);
			}

			this.progress.update();
			this.commit();
		}
	}

	private void findArcs (Sha256Hash senderTxIds, Long index, List<byte[]> receivers) throws RocksDBException {
		byte[] sender = TransactionAddresses.getAtOffset(this.mappings, senderTxIds, index.intValue());

		if (sender == null || sender == Parameters.MISSING_ADDRESS) {
			// null -> sender transaction now available
			// MA   -> sender address not "human"
			return;
		}

		Arcs.put(this.wb, this.arcs, sender, receivers);
	}

	public void commit () {
		if (this.wb.getDataSize() > Parameters.WRITE_BUFFER_SIZE) {
			this.wbQueue.add(this.wb);
			this.wb = new WriteBatch();
		}
	}
}
