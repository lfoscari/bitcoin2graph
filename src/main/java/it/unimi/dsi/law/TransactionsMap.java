package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsMap {
	void compute() throws IOException {
		Object2LongFunction<CharSequence> transactionMap = new Object2LongOpenHashMap<>();
		long count = 0;

		Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
		ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "transactions");
		progress.displayLocalSpeed = true;
		progress.start("Building address to long map");

		File[] inputs = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith("tsv"));

		if (inputs == null) {
			throw new NoSuchFileException("Download inputs first");
		}

		TSVDirectoryLineReader transactions = new TSVDirectoryLineReader(
			inputs, (line) -> true,
			(line) -> new String[] { line[BitcoinColumn.SPENDING_TRANSACTION_HASH] },
			true, null);

		while (true) {
			try {
				String transaction = transactions.next()[0];

				if (!transactionMap.containsKey(transaction)) {
					transactionMap.put(transaction, count++);
					progress.lightUpdate();
				}
			} catch (NoSuchElementException e) {
				break;
			}
		}

		progress.start("Saving transactions map");
		BinIO.storeObject(transactionMap, transactionsMapFile.toFile());
		progress.stop("Map saved in " + transactionsMapFile);
	}

	public static void main (String[] args) throws IOException {
		new TransactionsMap().compute();
	}
}
