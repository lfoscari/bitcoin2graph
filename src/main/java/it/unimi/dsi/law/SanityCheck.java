package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.longs.LongSortedSets.EmptySet;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.LongBinaryOperator;
import java.util.stream.LongStream;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.transactionOutputsFile;

public class SanityCheck {
	private static final int transactionAmount = 1_000_000;
	private static final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom();
	static ProgressLogger progress = new ProgressLogger(LoggerFactory.getLogger(SanityCheck.class), "transactions");
	static int errors = 0;
	static int notFound = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		/* Pick {transactionAmount} transactions at random and check that those transactions contain the right inputs
		 and outputs.
		  */

		GOVMinimalPerfectHashFunction<CharSequence> transactionsMap =
				(GOVMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(transactionsMapFile.toFile());
		GOVMinimalPerfectHashFunction<CharSequence> addressesMap =
				(GOVMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(addressesMapFile.toFile());
		Long2ObjectOpenHashMap<LongOpenHashSet> transactionInputs =
				(Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionInputsFile.toFile());
		Long2ObjectOpenHashMap<LongOpenHashSet> transactionOutputs =
				(Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionOutputsFile.toFile());

		progress.start("Picking " + transactionAmount + " random transactions");
		progress.expectedUpdates = transactionAmount;

		File[] transactionsFiles = transactionsDirectory.toFile().listFiles();
		if (transactionsFiles == null) throw new NoSuchElementException("No transactions found!");
		Utils.LineFilter transactionFilter = (line) -> Utils.column(line, 7).equals("0");
		Utils.TSVIterator transactions = new Utils.TSVIterator(transactionsFiles);

		CharSequence[] randomTransactions = new CharSequence[transactionAmount];
		long[] randomTransactionsId = new long[transactionAmount];
		File[] randomTransactionsFile = new File[transactionAmount];
		int index = 0;

		// Avoid duplicates
		LongOpenHashSet positionsSet = new LongOpenHashSet();
		while (positionsSet.size() < transactionAmount) {
			positionsSet.add(random.nextLong(transactionsMap.size64()));
		}

		long[] positions = positionsSet.toLongArray();
		Arrays.sort(positions);
		long[] offsets = new long[transactionAmount];

		offsets[0] = positions[0];
		for (int i = 1; i < transactionAmount; i++) {
			offsets[i] = positions[i] - positions[i - 1];
		}

		for(long skip: offsets) {
			MutableString line = transactions.next();
			while (--skip > 0|| !transactionFilter.accept(line)) {
				line = transactions.next();
			}

			randomTransactions[index] = new MutableString(Utils.column(line, 1));
			randomTransactionsId[index] = transactionsMap.getLong(randomTransactions[index]);
			randomTransactionsFile[index] = transactions.currentFile();
			index++;

			progress.update();
		}

		progress.stop();
		progress.start("Checking transaction inconsistencies");
		progress.expectedUpdates = transactionAmount;

		for (int i = 0; i < transactionAmount; i++) {
			progress.update();

			long transactionId = randomTransactionsId[i];
			final CharSequence transaction = randomTransactions[i];

			LongOpenHashSet inputs = transactionInputs.get(transactionId);
			LongOpenHashSet outputs = transactionOutputs.get(transactionId);

			File name = randomTransactionsFile[i];
			String date = name.getName().substring(name.getName().lastIndexOf("_"));

			Path associatedInput = inputsDirectory.resolve("blockchair_bitcoin_inputs" + date);
			Path associatedOutput = outputsDirectory.resolve("blockchair_bitcoin_outputs" + date);

			if (!associatedInput.toFile().exists() || !associatedOutput.toFile().exists()) {
				progress.logger.error("Associated input or output not found for " + name);
				notFound++;
				continue;
			}

			Iterator<MutableString> iterator = Utils.readTSVs(associatedInput);
			while (iterator.hasNext()) {
				MutableString line = iterator.next();

				if (transactionsMap.getLong(Utils.column(line, SPENDING_TRANSACTION_HASH)) != transactionId) {
					continue;
				}

				CharSequence address = Utils.column(line, RECIPIENT);
				long addressId = addressesMap.getLong(address);

				if (!inputs.contains(addressId)) {
					error("input", inputs, transactionId, transaction, name, associatedInput, address, addressId);
				}
			}

			Utils.LineFilter filter = (line) -> Utils.column(line, IS_FROM_COINBASE).equals("0");
			iterator = Utils.readTSVs(associatedOutput, filter);

			while (iterator.hasNext()) {
				MutableString line = iterator.next();

				if (transactionsMap.getLong(Utils.column(line, TRANSACTION_HASH)) != transactionId) {
					continue;
				}

				CharSequence address = Utils.column(line, RECIPIENT);
				long addressId = addressesMap.getLong(address);

				if (!outputs.contains(addressId)) {
					error("output", outputs, transactionId, transaction, name, associatedOutput, address, addressId);
				}
			}

		}

		progress.stop(transactionAmount + " transactions checked, " + errors + " inconsistencies were found " +
				"and " + notFound + " files were not found");
		progress.done();
	}

	private static void error(String source, LongOpenHashSet addressSet, long transactionId, CharSequence transaction,
	                          File transactionFile, Path associatedFile,
	                          CharSequence address, long addressId) {
		progress.logger.error(
				"inconsistency in " + source +
				"\naddresses: " + addressSet +
				"\naddress: " + address + " (" + addressId + ")" +
				"\ntransaction file: " + transactionFile +
				"\ntransaction: " + transaction + " (" + transactionId + ")" +
				"\nfile: " + associatedFile
		);
		errors++;
	}
}
