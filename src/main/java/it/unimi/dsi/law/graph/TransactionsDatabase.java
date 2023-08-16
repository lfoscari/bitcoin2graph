package it.unimi.dsi.law.graph;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.law.Utils;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static it.unimi.dsi.law.graph.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.graph.Parameters.*;
import static it.unimi.dsi.law.Utils.LineFilter;

public class TransactionsDatabase {
	private final ProgressLogger progress;
	private final GOV3Function<byte[]> addressMap;
	private final GOV3Function<byte[]> transactionMap;
	private long[][] transactionInputs;
	private long[][] transactionOutputs;

	public TransactionsDatabase(GOV3Function<byte[]> addressMap, GOV3Function<byte[]> transactionMap) throws IOException {
		this(addressMap, transactionMap, null);
	}

	public TransactionsDatabase(GOV3Function<byte[]> addressMap, GOV3Function<byte[]> transactionMap, ProgressLogger progress) throws IOException {
		this.addressMap = addressMap;
		this.transactionMap = transactionMap;
		this.progress = progress == null ? Utils.getProgressLogger(Blockchain2Webgraph.class, "sources") : progress;

		if (transactionInputsFile.toFile().exists()) {
			try {
				this.progress.logger.info("Loading transaction inputs from memory");
				this.transactionInputs = (long[][]) BinIO.loadObject(transactionInputsFile.toFile());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			this.computeInputs();
			BinIO.storeObject(this.transactionInputs, transactionInputsFile.toFile());
		}

		if (transactionOutputsFile.toFile().exists()) {
			try {
				this.progress.logger.info("Loading transaction outputs table from memory");
				this.transactionOutputs = (long[][]) BinIO.loadObject(transactionOutputsFile.toFile());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			this.computeOutputs();
			BinIO.storeObject(this.transactionOutputs, transactionOutputsFile.toFile());
		}
	}

	private void computeInputs() throws IOException {
		this.transactionInputs = new long[Math.toIntExact(this.transactionMap.size64())][0];
		int[] offsets = new int[Math.toIntExact(this.transactionMap.size64())];
		this.progress.start("Computing transaction inputs table");

		File[] sources = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) throw new NoSuchFileException("No inputs found in " + inputsDirectory);

		Utils.readTSVs(sources, null).forEachRemaining((s) -> {
			long addressId;
			int transactionId;
			try {
				addressId = this.addressMap.getLong(Utils.columnBytes(s, RECIPIENT));
				transactionId = (int) this.transactionMap.getLong(Utils.columnBytes(s, SPENDING_TRANSACTION_HASH));
			} catch (RuntimeException e) {
				this.progress.logger.error("Column number too high for line\n\t" + s);
				return;
			}

			if (addressId == this.addressMap.defaultReturnValue() || transactionId == this.transactionMap.defaultReturnValue()) {
				throw new RuntimeException("Unknown address " + Utils.column(s, RECIPIENT) + " (" + addressId + ") or transaction " + Utils.column(s, SPENDING_TRANSACTION_HASH) + " (" + transactionId + ")");
			}

			this.transactionInputs[transactionId] = LongArrays.ensureCapacity(this.transactionInputs[transactionId], this.transactionInputs[transactionId].length + 1);
			this.transactionInputs[transactionId][offsets[transactionId]++] = addressId;
			this.progress.lightUpdate();
		});
		this.progress.done();
	}

	private void computeOutputs() throws IOException {
		this.transactionOutputs = new long[Math.toIntExact(this.transactionMap.size64())][0];
		int[] offsets = new int[Math.toIntExact(this.transactionMap.size64())];

		LineFilter filter = (line) -> Utils.column(line, IS_FROM_COINBASE).equals("0");
		File[] sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) throw new NoSuchFileException("No outputs found in " + outputsDirectory);

		this.progress.start("Computing transaction outputs table");

		Utils.readTSVs(sources, filter).forEachRemaining((s) -> {
			long addressId;
			int transactionId;
			try {
				addressId = this.addressMap.getLong(Utils.columnBytes(s, RECIPIENT));
				transactionId = (int) this.transactionMap.getLong(Utils.columnBytes(s, TRANSACTION_HASH));
			} catch (RuntimeException e) {
				this.progress.logger.error("Column number too high for line\n\t" + s);
				return;
			}
			if (addressId == this.addressMap.defaultReturnValue() || transactionId == this.transactionMap.defaultReturnValue()) {
				throw new RuntimeException("Unknown address " + Utils.column(s, RECIPIENT) + " (" + addressId + ") or transaction " + Utils.column(s, TRANSACTION_HASH) + " (" + transactionId + ")");
			}

			this.transactionOutputs[transactionId] = LongArrays.ensureCapacity(this.transactionOutputs[transactionId], this.transactionOutputs[transactionId].length + 1);
			this.transactionOutputs[transactionId][offsets[transactionId]++] = addressId;
			this.progress.lightUpdate();
		});

		this.progress.done();
	}

	public long[] getInputAddresses(long transaction) {
		return this.transactionInputs[(int) transaction];
	}

	public long[] getOutputAddresses(long transaction) {
		return this.transactionOutputs[(int) transaction];
	}
}
