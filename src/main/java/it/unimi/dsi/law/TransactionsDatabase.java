package it.unimi.dsi.law;

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
	Long2ObjectOpenHashMap<LongList> transactionInputs = new Long2ObjectOpenHashMap<>();
	Long2ObjectOpenHashMap<LongList> transactionOutputs = new Long2ObjectOpenHashMap<>();

	public TransactionsDatabase (GOVMinimalPerfectHashFunction<MutableString> addressMap, GOVMinimalPerfectHashFunction<MutableString> transactionMap) {
		this(addressMap, transactionMap, null);
	}

	public TransactionsDatabase (GOVMinimalPerfectHashFunction<MutableString> addressMap, GOVMinimalPerfectHashFunction<MutableString> transactionMap, ProgressLogger progress) {
		this.addressMap = addressMap;
		this.transactionMap = transactionMap;
		this.progress = progress == null ? Utils.getProgressLogger(Blockchain2Webgraph.class, "sources") : progress;
	}

	void compute () throws IOException {
		this.progress.start("Building input transactions database");
		this.computeInputs();
		this.progress.stop();

		this.progress.start("Building output transactions database");
		this.computeOutputs();
		this.progress.done();
	}

	private void computeInputs() throws IOException {
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
