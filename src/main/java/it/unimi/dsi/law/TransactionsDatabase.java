package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsDatabase {
	private final ProgressLogger progress;
	private final GOVMinimalPerfectHashFunction<CharSequence> addressMap;
	private final GOVMinimalPerfectHashFunction<CharSequence> transactionMap;
	private Long2ObjectOpenHashMap<LongOpenHashSet> transactionInputs;
	private Long2ObjectOpenHashMap<LongOpenHashSet> transactionOutputs;

	public TransactionsDatabase (GOVMinimalPerfectHashFunction<CharSequence> addressMap, GOVMinimalPerfectHashFunction<CharSequence> transactionMap) throws IOException {
		this(addressMap, transactionMap, null);
	}

	public TransactionsDatabase (GOVMinimalPerfectHashFunction<CharSequence> addressMap, GOVMinimalPerfectHashFunction<CharSequence> transactionMap, ProgressLogger progress) throws IOException {
		this.addressMap = addressMap;
		this.transactionMap = transactionMap;
		this.progress = progress == null ? Utils.getProgressLogger(Blockchain2Webgraph.class, "sources") : progress;

		if (transactionInputsFile.toFile().exists()) {
			try {
				this.progress.logger.info("Loading transaction inputs from memory");
				this.transactionInputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionInputsFile.toFile());
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
				this.transactionOutputs = (Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionOutputsFile.toFile());
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
		this.transactionInputs = new Long2ObjectOpenHashMap<>();

		File[] sources = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No inputs found in " + inputsDirectory);
		}

		Utils.readTSVs(sources, new MutableString(), null).forEachRemaining((s) -> {
			long addressId = this.addressMap.getLong(Utils.column(s, RECIPIENT));
			long transactionId = this.transactionMap.getLong(Utils.column(s, SPENDING_TRANSACTION_HASH));

			if (addressId == -1 || transactionId == -1) {
				return;
			}

			this.add(this.transactionInputs, transactionId, addressId);
			this.progress.lightUpdate();
		});

		this.transactionInputs.trim();
	}

	private void computeOutputs() throws IOException {
		this.transactionOutputs = new Long2ObjectOpenHashMap<>();

		LineFilter filter = (line) -> Utils.columnEquals(line, IS_FROM_COINBASE, "0");
		File[] sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No outputs found in " + outputsDirectory);
		}

		Utils.readTSVs(sources, new MutableString(), filter).forEachRemaining((s) -> {
			long addressId = this.addressMap.getLong(Utils.column(s, RECIPIENT));
			long transactionId = this.transactionMap.getLong(Utils.column(s, TRANSACTION_HASH));

			if (addressId == -1 || transactionId == -1) {
				return;
			}

			this.add(this.transactionOutputs, transactionId, addressId);
			this.progress.lightUpdate();
		});

		this.transactionOutputs.trim();
	}

	public void add(Long2ObjectOpenHashMap<LongOpenHashSet> table, long transaction, long address) {
		table.compute(transaction, (k, v) -> {
			if (v == null) {
				return LongOpenHashSet.of(address);
			}

			v.add(address);
			return v;
		});
	}

	public LongOpenHashSet getInputAddresses (long transaction) {
		if (this.transactionInputs.containsKey(transaction)) {
			return this.transactionInputs.get(transaction);
		}

		return null;
	}

	public LongOpenHashSet getOutputAddresses (long transaction) {
		if (this.transactionOutputs.containsKey(transaction)) {
			return this.transactionOutputs.get(transaction);
		}

		return null;
	}
}
