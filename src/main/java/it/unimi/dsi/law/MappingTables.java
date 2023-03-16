package it.unimi.dsi.law;

import com.google.common.collect.Iterables;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static it.unimi.dsi.law.Parameters.*;

public class MappingTables {
	public static final Logger logger = LoggerFactory.getLogger(MappingTables.class);

	public static GOV3Function<byte[]> buildAddressesMap() throws IOException {
		artifacts.toFile().mkdir();

		if (addressesMapFile.toFile().exists()) {
			try {
				logger.info("Loading addresses mappings from memory");
				return (GOV3Function<byte[]>) BinIO.loadObject(addressesMapFile.toFile());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		logger.info("Computing addresses mappings");

		Iterable<byte[]> addresses = Iterables.transform(() -> Utils.readTSVs(addressesFile), line -> Utils.columnBytes(line, 0));
		return buildMap(addresses, addressesMapFile);
	}

	public static GOV3Function<byte[]> buildTransactionsMap() throws IOException {
		artifacts.toFile().mkdir();

		if (transactionsMapFile.toFile().exists()) {
			logger.info("Loading transactions mappings from memory");
			try {
				return (GOV3Function<byte[]>) BinIO.loadObject(transactionsMapFile.toFile());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		logger.info("Computing transactions mappings");
		Utils.LineFilter filter = (line) -> Utils.column(line, 7).equals("0");
		File[] sources = transactionsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
		if (sources == null) {
			throw new NoSuchFileException("No transactions found in " + transactionsDirectory);
		}

		Iterable<byte[]> transactions = Iterables.transform(() -> Utils.readTSVs(sources, filter), line -> Utils.columnBytes(line, 1));
		return buildMap(transactions, transactionsMapFile);
	}

	private static GOV3Function<byte[]> buildMap(Iterable<byte[]> keys, Path destination) throws IOException {
		File tempDir = Files.createTempDirectory(resources, "map_temp_").toFile();
		tempDir.deleteOnExit();

		GOV3Function<byte[]> map = new GOV3Function.Builder<byte[]>()
				.keys(keys)
				.tempDir(tempDir)
				.transform(TransformationStrategies.rawByteArray())
				.build();

		map.defaultReturnValue(-1);
		BinIO.storeObject(map, destination.toFile());
		return map;
	}

	public static void main(String[] args) throws IOException {
		MappingTables.buildAddressesMap();
		MappingTables.buildTransactionsMap();
	}
}
