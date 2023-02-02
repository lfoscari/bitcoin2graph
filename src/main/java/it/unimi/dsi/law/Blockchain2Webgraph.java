package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.EFGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;

public class Blockchain2Webgraph implements Iterator<long[]>, Iterable<long[]> {
	private final TransactionsDatabase transactionsDatabase;
	private final GOVMinimalPerfectHashFunction<MutableString> addressMap;
	private final GOVMinimalPerfectHashFunction<MutableString> transactionMap;
	private final Queue<long[]> arcs = new LinkedList<>();
	private final Iterator<MutableString> outputs;
	private final ProgressLogger progress;

	public Blockchain2Webgraph (TransactionsDatabase transactionsDatabase, GOVMinimalPerfectHashFunction<MutableString> addressMap, GOVMinimalPerfectHashFunction<MutableString> transactionMap, ProgressLogger progress) throws IOException {
		this.transactionsDatabase = transactionsDatabase;
		this.addressMap = addressMap;
		this.transactionMap = transactionMap;

		Utils.LineFilter filter = (line) -> Utils.columnEquals(line, IS_FROM_COINBASE, "0");
		File[] sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No outputs found in " + outputsDirectory);
		}
		this.outputs = Utils.readTSVs(sources, new MutableString(), filter, null);
		this.progress = progress == null ? Utils.getProgressLogger(Blockchain2Webgraph.class, "arcs") : progress;
	}

	@Override
	public Iterator<long[]> iterator () {
		return this;
	}

	@Override
	public boolean hasNext () {
		if (!this.arcs.isEmpty()) {
			return true;
		}

		while (this.outputs.hasNext()) {
			MutableString tsvLine = this.outputs.next();

			long transactionId = this.transactionMap.getLong(Utils.column(tsvLine, TRANSACTION_HASH));
			LongList inputAddresses = this.transactionsDatabase.getInputAddresses(transactionId);
			if (inputAddresses == null || inputAddresses.size() == 0) {
				continue;
			}

			long outputAddress = this.addressMap.getLong(Utils.column(tsvLine, RECIPIENT));
			for (long inputAddress : inputAddresses) {
				this.arcs.add(new long[] {inputAddress, outputAddress});
			}

			return true;
		}

		return false;
	}

	@Override
	public long[] next () {
		this.progress.lightUpdate();
		return this.arcs.remove();
	}

	public static void main (String[] args) throws IOException {
		ProgressLogger progress = Utils.getProgressLogger(Blockchain2Webgraph.class, "items");
		MappingTables mt = new MappingTables(progress);
		GOVMinimalPerfectHashFunction<MutableString> addressMap = mt.buildAddressesMap();
		GOVMinimalPerfectHashFunction<MutableString> transactionMap = mt.buildTransactionsMap();

		TransactionsDatabase transactions = new TransactionsDatabase(addressMap, transactionMap);
		transactions.compute();

		progress = Utils.getProgressLogger(Blockchain2Webgraph.class, "arcs");
		graph.toFile().mkdir();

		Blockchain2Webgraph bw = new Blockchain2Webgraph(transactions, addressMap, transactionMap, progress);
		File tempDir = Files.createTempDirectory(resources, "bw_temp").toFile();
		tempDir.deleteOnExit();

		ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bw.iterator(), false, false, 100_000, tempDir, progress);

		EFGraph.store(graph, basename.toString());
		BinIO.storeLongs(graph.ids, ids.toFile());
	}
}
