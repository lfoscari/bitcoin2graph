package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.EFGraph;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.transactionOutputsFile;

public class SanityCheck {
	private static int transactionAmount = 10_000;
	private static final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom();
	private static final ProgressLogger progress = new ProgressLogger(LoggerFactory.getLogger(SanityCheck.class), "transactions");
	private static int missingInputsOutputs = 0, missingNodes = 0, notFound = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		/* Pick {transactionAmount} transactions at random and check that those transactions contain the right inputs
		 and outputs.

		 Note that given a transaction processed on a specific day, it's not a given that on the
		 corresponding input or output there will be any information about the transaction. That's why we distinguish
		 between error and the case in which the inputs or outputs were not found, it's easier to just skip the
		 transaction. */

		progress.logger.info("Loading transactions map");
		GOVMinimalPerfectHashFunction<CharSequence> transactionsMap =
				(GOVMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(transactionsMapFile.toFile());
		progress.logger.info("Loading addresses map");
		GOVMinimalPerfectHashFunction<CharSequence> addressesMap =
				(GOVMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(addressesMapFile.toFile());
		progress.logger.info("Loading transactions inputs");
		Long2ObjectOpenHashMap<LongOpenHashSet> transactionInputs =
				(Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionInputsFile.toFile());
		progress.logger.info("Loading transactions outputs");
		Long2ObjectOpenHashMap<LongOpenHashSet> transactionOutputs =
				(Long2ObjectOpenHashMap<LongOpenHashSet>) BinIO.loadObject(transactionOutputsFile.toFile());
		progress.logger.info("Loading graph");
		EFGraph graph = EFGraph.load(basename.toString());
		progress.logger.info("Loading graph ids");
		long[] nodeIds = BinIO.loadLongs(ids.toString());

		// cover different test sets
		transactionAmount = Integer.min(transactionAmount, Math.toIntExact(transactionsMap.size64()) / 2);

		progress.start("Picking " + transactionAmount + " random transactions");
		progress.logInterval = TimeUnit.MINUTES.toMillis(1);
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
		progress.logInterval = TimeUnit.MINUTES.toMillis(30);
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
					reportInconsistency("input", inputs, transactionId, transaction, name, associatedInput, address, addressId);
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
					reportInconsistency("output", outputs, transactionId, transaction, name, associatedOutput, address, addressId);
				}
			}

			// Check that each input has among its successors all the outputs in the graph
			for (long inputAddress: inputs) {
				int inputAddressNode = ArrayUtils.indexOf(nodeIds, inputAddress);

				int[] successors = graph.successorArray(inputAddressNode);
				long[] addressSuccessors = new long[graph.outdegree(inputAddressNode)];
				for (int k = 0; k < addressSuccessors.length; k++) {
					addressSuccessors[k] = nodeIds[successors[k]];
				}

				for (long outputAddress: outputs) {
					if (!ArrayUtils.contains(addressSuccessors, outputAddress)) {
						reportMissingNode(transaction, inputs, outputs, inputAddress, successors, outputAddress);
						break;
					}
				}
			}
		}

		progress.stop(transactionAmount + " transactions checked, " + missingInputsOutputs + " inconsistencies were found, " +
				missingNodes + " addresses were incorrectly linked in the graph and " + notFound + " files were not found");
		progress.done();
	}

	private static void reportMissingNode(CharSequence transaction, LongOpenHashSet inputs, LongOpenHashSet outputs, long inputAddress, int[] successors, long outputAddress) {
		progress.logger.error(
				"Output not found analysing transaction " + transaction +
				"\nInputs: " + inputs + " (checking " + inputAddress + ")" +
				"\nOutputs: " + outputs + " (checking " + outputAddress + ")" +
				"\nSuccessors of the input node: " + Arrays.toString(successors));
		missingNodes++;
	}

	private static void reportInconsistency(String source, LongOpenHashSet addressSet, long transactionId, CharSequence transaction,
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
		missingInputsOutputs++;
	}
}
