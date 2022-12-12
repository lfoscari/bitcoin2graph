package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.ints.Int2LongFunction;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static it.unimi.dsi.law.Parameters.*;

public class AddressMap {
	void compute() throws IOException {
		Int2LongFunction addressMap = new Int2LongOpenHashMap();
		long count = 0;

		Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
		ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "addresses");
		progress.start("Building address to long map");

		for (String[] line : Utils.readTSVs(addressesFile.toFile())) {
			int address = line[0].hashCode();
			addressMap.put(address, count++);
			progress.lightUpdate();
		}

		progress.start("Saving address map (total " + count + " addresses)");
		BinIO.storeObject(addressMap, addressesMapFile.toFile());
		progress.stop("Map saved in " + addressesMapFile);
	}

	public static void main (String[] args) throws IOException {
		new AddressMap().compute();
	}
}
