package it.unimi.dsi.law.graph;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static it.unimi.dsi.law.graph.Parameters.*;

/** Note: you can simply use <code>java it.unimi.dsi.sux4j.mph.GOV3Function -s 10 -b -T . address.map addresses.tsv</code>
 * and will likely be faster */
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

		Iterable<byte[]> addresses = StreamSupport.stream(new FileLinesMutableStringIterable(addressesFile.toString()).spliterator(), false)
				.map(line -> line.toString().getBytes()).collect(Collectors.toList());
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
		Iterable<byte[]> transactions = StreamSupport.stream(new FileLinesMutableStringIterable(transactionsFile.toString()).spliterator(), false)
				.map(line -> line.toString().getBytes()).collect(Collectors.toList());
		return buildMap(transactions, transactionsMapFile);
	}

	private static GOV3Function<byte[]> buildMap(Iterable<byte[]> keys, Path destination) throws IOException {
		GOV3Function<byte[]> map = new GOV3Function.Builder<byte[]>()
				.keys(keys)
				.transform(TransformationStrategies.rawByteArray())
				.signed(10)
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
