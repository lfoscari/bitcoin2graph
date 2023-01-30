package it.unimi.dsi.law;

import it.unimi.dsi.lang.MutableString;
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
			Logger logger = LoggerFactory.getLogger(TransactionsDatabase.class);
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "sources");
			progress.displayLocalSpeed = true;
		}

		this.progress = progress;
	}

	void compute () throws IOException, RocksDBException {
		try (RocksDBWrapper database = new RocksDBWrapper(false, transactionsDatabaseDirectory)) {
			this.progress.start("Building output transactions database");

			{
				LineFilter filter = (line) -> Utils.column(line, IS_FROM_COINBASE).equals("0");
				File[] sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
				if (sources == null) {
					throw new NoSuchFileException("No outputs found in " + outputsDirectory);
				}

				for (MutableString tsvLine : Utils.readTSVs(sources, new MutableString(), filter)) {
					long addressId = Utils.hashCode(Utils.column(tsvLine, RECIPIENT));
					long transactionId = Utils.hashCode(Utils.column(tsvLine, TRANSACTION_HASH));

					database.add(OUTPUT, Utils.longToBytes(transactionId), Utils.longToBytes(addressId));
					this.progress.lightUpdate();
				}
			}

			database.commit();
			this.progress.stop();
			this.progress.start("Building inputs database");

			{
				File[] sources = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
				if (sources == null) {
					throw new NoSuchFileException("No inputs found in " + inputsDirectory);
				}

				for (MutableString tsvLine : Utils.readTSVs(sources, new MutableString(), null)) {
					long inputAddress = Utils.hashCode(Utils.column(tsvLine, RECIPIENT));

					long spendingTransaction = Utils.hashCode(Utils.column(tsvLine, SPENDING_TRANSACTION_HASH));
					byte[] outputAddresses = database.get(OUTPUT,  Utils.longToBytes(spendingTransaction));

					if (outputAddresses == null) {
						// No common arcs
						continue;
					}

					database.add(INPUT, Utils.longToBytes(inputAddress), outputAddresses);
					this.progress.lightUpdate();
				}
			}

			this.progress.done();
		}
	}

	public static void main (String[] args) throws IOException, RocksDBException {
		new TransactionsDatabase().compute();
	}
}
