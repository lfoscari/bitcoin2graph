package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
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
	private final GOVMinimalPerfectHashFunction<MutableString> addressMap;
	private final GOVMinimalPerfectHashFunction<MutableString> transactionMap;
	private Long2ObjectOpenHashMap<LongList> transactionInputs;
	private Long2ObjectOpenHashMap<LongList> transactionOutputs;

	public TransactionsDatabase (GOVMinimalPerfectHashFunction<MutableString> addressMap, GOVMinimalPerfectHashFunction<MutableString> transactionMap) throws IOException {
		this(addressMap, transactionMap, null);
	}

	public TransactionsDatabase (GOVMinimalPerfectHashFunction<MutableString> addressMap, GOVMinimalPerfectHashFunction<MutableString> transactionMap, ProgressLogger progress) throws IOException {
		this.addressMap = addressMap;
		this.transactionMap = transactionMap;
		this.progress = progress == null ? Utils.getProgressLogger(Blockchain2Webgraph.class, "sources") : progress;

		try {
			this.transactionInputs = (Long2ObjectOpenHashMap<LongList>) BinIO.loadObject(transactionInputsFile.toFile());
			this.progress.logger.info("Loaded transaction inputs from memory");
		} catch (IOException e) {
			this.progress.start("Computing transaction inputs table");
			this.computeInputs();
			BinIO.storeObject(this.transactionInputs, transactionInputsFile.toFile());
			this.progress.done();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		try {
			this.transactionOutputs = (Long2ObjectOpenHashMap<LongList>) BinIO.loadObject(transactionOutputsFile.toFile());
			this.progress.logger.info("Loaded transaction outputs table from memory");
		} catch (IOException e) {
			this.progress.start("Computing transaction outputs table");
			this.computeOutputs();
			BinIO.storeObject(this.transactionOutputs, transactionOutputsFile.toFile());
			this.progress.done();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	private void computeInputs() throws IOException {
		this.transactionInputs = new Long2ObjectOpenHashMap<>();

		File[] sources = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No inputs found in " + inputsDirectory);
		}

		Utils.readTSVs(sources, new MutableString(), null, null).forEachRemaining((s) -> {
			long addressId = this.addressMap.getLong(Utils.column(s, RECIPIENT));
			long transactionId = this.transactionMap.getLong(Utils.column(s, SPENDING_TRANSACTION_HASH));

			this.add(this.transactionInputs, transactionId, addressId);
			this.progress.lightUpdate();
		});
	}

	private void computeOutputs() throws IOException {
		this.transactionOutputs = new Long2ObjectOpenHashMap<>();

		LineFilter filter = (line) -> Utils.columnEquals(line, IS_FROM_COINBASE, "0");
		File[] sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No outputs found in " + outputsDirectory);
		}

		Utils.readTSVs(sources, new MutableString(), filter, null).forEachRemaining((s) -> {
			long addressId = this.addressMap.getLong(Utils.column(s, RECIPIENT));
			long transactionId = this.transactionMap.getLong(Utils.column(s, TRANSACTION_HASH));

			this.add(this.transactionOutputs, transactionId, addressId);
			this.progress.lightUpdate();
		});
	}

	public void add(Long2ObjectOpenHashMap<LongList> table, long transaction, long address) {
		table.compute(transaction, (k, v) -> {
			if (v == null) {
				return LongArrayList.of(address);
			}

			v.add(address);
			return v;
		});
	}

	public LongList getInputAddresses (long transaction) {
		if (this.transactionInputs.containsKey(transaction)) {
			return this.transactionInputs.get(transaction);
		}

		return null;
	}

	public LongList getOutputAddresses (long transaction) {
		if (this.transactionOutputs.containsKey(transaction)) {
			return this.transactionOutputs.get(transaction);
		}

		return null;
	}
}
