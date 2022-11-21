package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.law.utils.BitcoinUtils;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * Idea per parallelizzare: ogni thread considera una parte della blockchain
 * e testa di completare i mappings, restituisce una mappa come this.maps e
 * una lista come this.incomplete. A questo punto basta lanciare altri thread
 * su this.incomplete utilizzando le informazioni di this.map e si chiude tutto.
 */

public class OnlineParsing {
	private final Map<byte[], byte[][]> map;
	private final LinkedBlockingQueue<Pair<byte[], byte[]>> arcs;
	private final List<Transaction> incomplete;
	private final List<File> blockFiles;
	private final NetworkParameters np;

	private final ArcsBackup arcsBackup;
	private final Thread arcsBackupThread;

	public static void main (String[] args) throws InterruptedException, FileNotFoundException {
		OnlineParsing op = new OnlineParsing();
		op.run();
	}

	public OnlineParsing () throws FileNotFoundException {
		this.map = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
		this.arcs = new LinkedBlockingQueue<>();

		this.incomplete = new ObjectArrayList<>();
		this.blockFiles = BitcoinUtils.getBlockFiles(Parameters.resources + "blocks");

		this.np = new MainNetParams();
		new Context(this.np);

		this.arcsBackup = new ArcsBackup(this.arcs, this.np);
		this.arcsBackupThread = new Thread(this.arcsBackup);

		this.arcsBackupThread.start();
	}

	public void run () throws InterruptedException {
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

		System.out.println("Finishing saving results...");

		this.arcsBackup.stop = true;
		this.arcsBackupThread.join();

		System.out.println("Missing transactions " + this.incomplete.size());
		System.out.println("Found " + this.arcs.size() + " arcs");

		// this.saveArcs();
		// this.saveMap();
	}

	public void parseTransaction (Transaction transaction, byte[][] outputs) throws InterruptedException {
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
				this.arcs.put(Pair.of(senderAddress, outputAddress));
			}
		}
	}

	public void saveArcs (File destination) {
		try (FileOutputStream os = new FileOutputStream(destination)) {
			this.arcs.forEach(arc -> {
				try {
					byte[] sender = arc.left(), receiver = arc.right();
					os.write((BitcoinUtils.addressToString(sender, this.np) + " -> " + BitcoinUtils.addressToString(receiver, this.np) + "\n").getBytes());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void saveMap (File destination) {
		try (FileOutputStream os = new FileOutputStream(destination)) {
			this.map.forEach((txIdBytes, addressesBytes) -> {
				String txId = Sha256Hash.wrap(txIdBytes).toString();

				StringBuilder addresses = new StringBuilder();
				for (byte[] address : addressesBytes) {
					addresses.append(BitcoinUtils.addressToString(address, this.np)).append(" ");
				}

				try {
					os.write((txId + " -> " + addresses + "\n").getBytes());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
