package it.unimi.dsi.law.utils;

import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptException;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static it.unimi.dsi.law.Parameters.MISSING_ADDRESS;
import static it.unimi.dsi.law.Parameters.UNKNOWN_ADDRESS;
import static org.bitcoinj.core.Message.UNKNOWN_LENGTH;

public class BitcoinUtils {
	public static List<Transaction> getTransactions (byte[] block, NetworkParameters np) {
		int cursor = Block.HEADER_SIZE;
		VarInt transactionsAmount = new VarInt(block, cursor);
		cursor += transactionsAmount.getOriginalSizeInBytes();

		if (block.length == cursor) {
			// This message is just a header, it has no transactions.
			return List.of();
		}

		int transactionAmountInt;
		try {
			transactionAmountInt = transactionsAmount.intValue();
		} catch (IllegalArgumentException e) {
			return null;
		}

		List<Transaction> transactions = new ArrayList<>(Math.min(transactionAmountInt, Utils.MAX_INITIAL_ARRAY_LENGTH));
		for (int i = 0; i < transactionAmountInt; i++) {
			Transaction tx = new Transaction(np, block, cursor, null, np.getDefaultSerializer(), UNKNOWN_LENGTH, null);
			transactions.add(tx);
			cursor += tx.getMessageSize();
		}

		return transactions;
	}

	public static List<File> getBlockFiles (String blocksDirName) {
		File blocksDir = new File(blocksDirName);
		List<File> list = new ArrayList<>();
		for (int i = 0; true; i++) {
			File file = new File(blocksDir, String.format(Locale.US, "blk%05d.dat", i));
			if (!file.exists()) {
				break;
			}
			list.add(file);
		}
		return list;
	}

	public static String addressToString (byte[] address, NetworkParameters np) {
		if (address == MISSING_ADDRESS) {
			return "MISSING";
		} else if (address == UNKNOWN_ADDRESS) {
			return "UNKNOWN";
		} else if (address.length == LegacyAddress.LENGTH) {
			return LegacyAddress.fromPubKeyHash(np, address).toBase58();
		} else if (address.length == SegwitAddress.WITNESS_PROGRAM_LENGTH_TR) {
			return "SEGWIT=" + SegwitAddress.fromHash(np, address);
		}

		throw new RuntimeException("Unknown address format! " + Arrays.toString(address));
	}

	public static byte[][] getOutputAddresses (Transaction t, NetworkParameters np) {
		return t.getOutputs()
				.stream()
				.map((TransactionOutput to) -> transactionOutputToAddress(to, np))
				.toArray(byte[][]::new);
	}

	public static byte[] transactionOutputToAddress (TransactionOutput to, NetworkParameters np) {
		try {
			Script script = to.getScriptPubKey();

			if (script.getScriptType() == null) {
				// No public keys are contained in this script.
				return MISSING_ADDRESS;
			}

			return script.getToAddress(np, true).getHash();
		} catch (IllegalArgumentException | ScriptException e) {
			// Non-standard address
			return UNKNOWN_ADDRESS;
		}
	}
}
