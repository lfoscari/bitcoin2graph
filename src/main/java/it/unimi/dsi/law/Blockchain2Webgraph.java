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
	private final GOVMinimalPerfectHashFunction<CharSequence> transactionMap;
	private final Queue<long[]> arcs = new LinkedList<>();
	private final Iterator<MutableString> transactions;
	private final ProgressLogger progress;

	public Blockchain2Webgraph (TransactionsDatabase transactionsDatabase, GOVMinimalPerfectHashFunction<CharSequence> transactionMap, ProgressLogger progress) throws IOException {
		this.transactionsDatabase = transactionsDatabase;
		this.transactionMap = transactionMap;

		Utils.LineFilter filter = (line) -> Utils.columnEquals(line, 7, "0");
		File[] sources = transactionsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No transactions found in " + transactionsDirectory);
		}
		this.transactions = Utils.readTSVs(sources, new MutableString(), filter);
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

		while (this.transactions.hasNext()) {
			CharSequence transaction = Utils.column(this.transactions.next(), 1);
			long transactionId = this.transactionMap.getLong(transaction);

			LongList inputAddresses = this.transactionsDatabase.getInputAddresses(transactionId);
			LongList outputAddresses = this.transactionsDatabase.getOutputAddresses(transactionId);

			if (inputAddresses == null || outputAddresses == null) {
				continue;
			}

			for (long inputAddress : inputAddresses) {
				for (long outputAddress : outputAddresses) {
					this.arcs.add(new long[] { inputAddress, outputAddress });
				}
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
		graph.toFile().mkdir();
		artifacts.toFile().mkdir();

		GOVMinimalPerfectHashFunction<CharSequence> addressMap = MappingTables.buildAddressesMap();
		GOVMinimalPerfectHashFunction<CharSequence> transactionMap = MappingTables.buildTransactionsMap();

		TransactionsDatabase transactions = new TransactionsDatabase(addressMap, transactionMap);

		ProgressLogger progress = Utils.getProgressLogger(Blockchain2Webgraph.class, "arcs");
		Blockchain2Webgraph bw = new Blockchain2Webgraph(transactions, transactionMap, progress);
		File tempDir = Files.createTempDirectory(resources, "bw_temp").toFile();
		tempDir.deleteOnExit();

		ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bw.iterator(), false, false, 100_000, tempDir, progress);

		EFGraph.store(graph, basename.toString());
		BinIO.storeLongs(graph.ids, ids.toFile());
	}
}
