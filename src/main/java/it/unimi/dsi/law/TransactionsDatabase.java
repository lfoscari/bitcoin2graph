package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.LineFilter;

public class TransactionsDatabase {
	private final ProgressLogger progress;
	private final GOV3Function<byte[]> addressMap;
	private final GOV3Function<byte[]> transactionMap;
	private Long2ObjectArrayMap<LongOpenHashSet> transactionInputs;
	private Long2ObjectArrayMap<LongOpenHashSet> transactionOutputs;

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
				this.transactionInputs = (Long2ObjectArrayMap<LongOpenHashSet>) BinIO.loadObject(transactionInputsFile.toFile());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			this.progress.start("Computing transaction inputs table");
			this.computeInputs();
			BinIO.storeObject(this.transactionInputs, transactionInputsFile.toFile());
			this.progress.done();
		}

		if (transactionOutputsFile.toFile().exists()) {
			try {
				this.progress.logger.info("Loading transaction outputs table from memory");
				this.transactionOutputs = (Long2ObjectArrayMap<LongOpenHashSet>) BinIO.loadObject(transactionOutputsFile.toFile());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			this.progress.start("Computing transaction outputs table");
			this.computeOutputs();
			BinIO.storeObject(this.transactionOutputs, transactionOutputsFile.toFile());
			this.progress.done();
		}
	}

	private void computeInputs() throws IOException {
		this.transactionInputs = new Long2ObjectArrayMap<>(Math.toIntExact(this.transactionMap.size64()));

		File[] sources = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No inputs found in " + inputsDirectory);
		}

		Utils.readTSVs(sources, null).forEachRemaining((s) -> {
			long addressId = this.addressMap.getLong(Utils.columnBytes(s, RECIPIENT));
			long transactionId = this.transactionMap.getLong(Utils.columnBytes(s, SPENDING_TRANSACTION_HASH));

			if (addressId == this.addressMap.defaultReturnValue() || transactionId == this.transactionMap.defaultReturnValue()) {
				throw new RuntimeException("Unknown address or transaction");
			}

			this.add(this.transactionInputs, transactionId, addressId);
			this.progress.lightUpdate();
		});
	}

	private void computeOutputs() throws IOException {
		this.transactionOutputs = new Long2ObjectArrayMap<>(Math.toIntExact(this.transactionMap.size64()));

		LineFilter filter = (line) -> Utils.column(line, IS_FROM_COINBASE).equals("0");
		File[] sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No outputs found in " + outputsDirectory);
		}

		Utils.readTSVs(sources, filter).forEachRemaining((s) -> {
			long addressId = this.addressMap.getLong(Utils.columnBytes(s, RECIPIENT));
			long transactionId = this.transactionMap.getLong(Utils.columnBytes(s, TRANSACTION_HASH));

			if (addressId == this.addressMap.defaultReturnValue() || transactionId == this.transactionMap.defaultReturnValue()) {
				throw new RuntimeException("Unknown address or transaction");
			}

			this.add(this.transactionOutputs, transactionId, addressId);
			this.progress.lightUpdate();
		});
	}

	public void add(Long2ObjectArrayMap<LongOpenHashSet> table, long transaction, long address) {
		table.compute(transaction, (k, v) -> {
			if (v == null) {
				return LongOpenHashSet.of(address);
			}

			v.add(address);
			return v;
		});
	}

	public LongOpenHashSet getInputAddresses(long transaction) {
		if (this.transactionInputs.containsKey(transaction)) {
			return this.transactionInputs.get(transaction);
		}

		return null;
	}

	public LongOpenHashSet getOutputAddresses(long transaction) {
		if (this.transactionOutputs.containsKey(transaction)) {
			return this.transactionOutputs.get(transaction);
		}

		return null;
	}
}
