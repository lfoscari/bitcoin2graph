package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.Parameters.*;

public class FindMapping implements Runnable {
	private final LinkedBlockingQueue<long[]> arcs;

	private final RocksDB inputs;
	private final RocksDB outputs;

	private final ProgressLogger progress;

	public FindMapping () throws RocksDBException {
		this(null, null);
	}

	public FindMapping (final LinkedBlockingQueue<long[]> arcs, ProgressLogger progress) throws RocksDBException {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(this.getClass());
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "transaction");
		}

		this.arcs = arcs;

		this.inputs = Utils.startDatabase(true, inputTransactionDatabaseDirectory);
		this.outputs = Utils.startDatabase(true, outputTransactionDatabaseDirectory);

		this.progress = progress;
	}

	public void run () {
		try {
			this.progress.start("Searching mappings...");
			this.findMapping();
			this.close();
			this.progress.done();
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void findMapping () throws IOException, ClassNotFoundException, InterruptedException {
		try (RocksIterator inputIterator = this.inputs.newIterator()) {
			for (inputIterator.seekToFirst(); inputIterator.isValid(); inputIterator.next()) {
				byte[] transaction = inputIterator.key();

				byte[] outputs = this.outputs.get(transaction);
				if (outputs == null) {
					continue;
				}

				long[] inputsAddresses = Utils.bytesToLongs(inputIterator.value());
				long[] outputsAddresses = Utils.bytesToLongs(outputs);

				for (long inputAddress : inputsAddresses) {
					for (long outputAddress : outputsAddresses) {
						if (this.arcs != null) {
							this.arcs.put(new long[] { inputAddress, outputAddress });
						} else {
							System.out.println(Arrays.toString(transaction) + ": " + inputAddress + " ~> " + outputAddress);
						}
					}
				}
			}
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	private void close () {
		this.inputs.close();
		this.outputs.close();
	}

	public static void main (String[] args) throws RocksDBException {
		new FindMapping().run();
	}
}
