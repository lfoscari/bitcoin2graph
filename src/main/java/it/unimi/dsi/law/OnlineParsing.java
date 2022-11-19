package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.law.utils.BitcoinUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class OnlineParsing {
	private final Map<byte[], byte[][]> map;
	private final MultiValuedMap<byte[], byte[]> arcs;
	private final List<Transaction> incomplete;
	private final List<File> blockFiles;
	private final NetworkParameters np;

	public static void main (String[] args) {
		OnlineParsing op = new OnlineParsing();
		op.run();
	}

	public OnlineParsing () {
		this.map = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
		this.arcs = new HashSetValuedHashMap<>();
		this.incomplete = new ObjectArrayList<>();
		this.blockFiles = BitcoinUtils.getBlockFiles(Parameters.resources + "blocks");

		this.np = new MainNetParams();
		new Context(this.np);
	}

	public void run () {
		BlockLoader bl = new BlockLoader(this.blockFiles, null, null, this.np);

		for (byte[] blockBytes : bl) {
			List<Transaction> transactions = BitcoinUtils.getTransactions(blockBytes, this.np);

			if (transactions == null) {
				System.out.println("Couldn't parse the transactions in a block!");
				continue;
			}

			for (Transaction transaction : transactions) {
				byte[][] outputs = BitcoinUtils.getOutputAddresses(transaction, this.np);
				this.map.put(transaction.getTxId().getBytes(), outputs);
				this.parseTransaction(transaction, outputs);
			}
		}

		// Complete missing transactions
		for (Transaction transaction : this.incomplete) {
			byte[][] outputs = BitcoinUtils.getOutputAddresses(transaction, this.np);
			this.parseTransaction(transaction, outputs);
		}

		System.out.println("Missing transactions " + this.incomplete.size());
		System.out.println("Found " + this.arcs.size() + " arcs");

		this.saveArcs();
		// this.saveMap();
	}

	public void parseTransaction (Transaction transaction, byte[][] outputs) {
		if (transaction.isCoinBase()) {
			return;
		}

		for (TransactionInput source : transaction.getInputs()) {
			TransactionOutPoint top = source.getOutpoint();
			Sha256Hash txId = top.getHash();
			long index = top.getIndex();

			byte[][] sendersAddresses = this.map.get(txId.getBytes());

			if (sendersAddresses == null) {
				this.incomplete.add(transaction);
				continue;
			}

			byte[] senderAddress = sendersAddresses[(int) index];

			for (byte[] outputAddress : outputs) {
				this.arcs.put(senderAddress, outputAddress);
			}
		}
	}

	public void saveArcs () {
		File arcs = new File(Parameters.resources + "arcs.txt");
		try (FileOutputStream os = new FileOutputStream(arcs);
			 FastBufferedOutputStream bos = new FastBufferedOutputStream(os)) {
			this.arcs.asMap().forEach((sender, receivers) -> {
				try {
					bos.write((BitcoinUtils.addressToString(sender, this.np) + " -> ").getBytes());
					for (byte[] receiver : receivers) {
						bos.write((BitcoinUtils.addressToString(receiver, this.np) + " ").getBytes());
					}
					bos.write("\n".getBytes());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void saveMap () {
		File map = new File(Parameters.resources + "map.txt");
		try (FileOutputStream os = new FileOutputStream(map);
			 FastBufferedOutputStream bos = new FastBufferedOutputStream(os)) {
			this.map.forEach((txIdBytes, addressesBytes) -> {
				String txId = Sha256Hash.wrap(txIdBytes).toString();

				StringBuilder addresses = new StringBuilder();
				for (byte[] address : addressesBytes) {
					addresses.append(BitcoinUtils.addressToString(address, this.np)).append(" ");
				}

				try {
					bos.write((txId + " -> " + addresses + "\n").getBytes());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
