package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.Parameters.*;

public class FindMapping implements Runnable {
	private final LinkedBlockingQueue<long[]> arcs;
	private final ProgressLogger progress;

	public FindMapping () {
		this(null, null);
	}

	public FindMapping (final LinkedBlockingQueue<long[]> arcs, ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(ParseTSVs.class);
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "transaction");
		}

		this.arcs = arcs;
		this.progress = progress;
	}

	public void run () {
		try {
			this.progress.start("Searching mappings...");
			this.findMapping();
			this.progress.done();
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void findMapping () throws IOException, ClassNotFoundException, InterruptedException {
		File[] transactionsInputs = inputTransactionDatabaseDirectory.toFile().listFiles();

		if (transactionsInputs == null) {
			throw new FileNotFoundException("No inputs found!");
		}

		for (File transactionInput : transactionsInputs) {
			this.progress.lightUpdate();
			String transaction = transactionInput.getName();
			File transactionOutput = outputTransactionDatabaseDirectory.resolve(transaction).toFile();

			if (!transactionOutput.exists()) {
				continue;
			}

			long[] inputAddresses = (long[]) BinIO.loadObject(transactionInput);
			long[] outputAddresses = (long[]) BinIO.loadObject(transactionOutput);

			for (long input : inputAddresses) {
				for (long output : outputAddresses) {
					if (this.arcs == null) {
						System.out.println(transaction + ": " + input + " -> " + output);
					} else {
						this.arcs.put(new long[]{input, output});
					}
				}
			}
		}
	}

	public static void main (String[] args) {
		new FindMapping().run();
	}
}
