package it.unimi.dsi.law;

import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.RocksDBWrapper.Column.INPUT;
import static it.unimi.dsi.law.RocksDBWrapper.Column.OUTPUT;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsDatabase {
	private final ProgressLogger progress;

	public TransactionsDatabase () {
		this(null);
	}

	public TransactionsDatabase (ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "sources");
			progress.displayLocalSpeed = true;
		}

		this.progress = progress;
	}

	void compute () throws IOException, RocksDBException {
		try (RocksDBWrapper database = new RocksDBWrapper(false, transactionsDatabaseDirectory)) {
			this.progress.start("Building input transactions database");

			LineCleaner cleaner = (line) -> Utils.keepColumns(line, INPUTS_IMPORTANT);
			File[] sources = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));

			if (sources == null) {
				throw new NoSuchFileException("Download inputs and outputs first");
			}

			for (String[] line : Utils.readTSVs(sources, null, cleaner)) {
				long addressId = Utils.hashCode(line[0]);
				long transactionId = Utils.hashCode(line[1]);

				database.add(INPUT, Utils.longToBytes(transactionId), Utils.longToBytes(addressId));
				this.progress.lightUpdate();
			}

			this.progress.stop();
			this.progress.start("Building output transactions database");

			LineFilter filter = (line) -> Utils.equalsAtColumn(line, "0", IS_FROM_COINBASE);
			cleaner = (line) -> Utils.keepColumns(line, OUTPUTS_IMPORTANT);
			sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));

			if (sources == null) {
				throw new NoSuchFileException("Download inputs and outputs first");
			}

			for (String[] line : Utils.readTSVs(sources, filter, cleaner)) {
				long addressId = Utils.hashCode(line[1]);
				long transactionId = Utils.hashCode(line[0]);

				database.add(OUTPUT, Utils.longToBytes(transactionId), Utils.longToBytes(addressId));
				this.progress.lightUpdate();
			}

			this.progress.done();
		}
	}

	public static void main (String[] args) throws IOException, RocksDBException {
		new TransactionsDatabase().compute();
	}
}
