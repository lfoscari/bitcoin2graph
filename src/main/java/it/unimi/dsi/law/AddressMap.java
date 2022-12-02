package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.law.Utils.TSVDirectoryLineReader;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.StringMap;
import it.unimi.dsi.util.StringMaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.AddressMap.compute;
import static it.unimi.dsi.law.Parameters.addressesFile;

public class AddressMap {
	static void compute() throws IOException {
		Object2LongFunction<String> addressMap = new Object2LongOpenHashMap<>();
		long count = 0;

		Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
		ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "addresses");
		progress.displayLocalSpeed = true;
		progress.start("Building address to long map");

		TSVDirectoryLineReader addresses = new TSVDirectoryLineReader(new File[] {addressesFile.toFile()}, (l) -> true, (l) -> l, null);

		while (true) {
			try {
				String address = addresses.next()[0];
				addressMap.put(address, count++);
				progress.lightUpdate();
			} catch (NoSuchElementException e) {
				break;
			}
		}

		progress.done();
		BinIO.storeObject(addressMap, addressesFile.toFile());
		progress.stop("Map saved in " + addressesFile);
	}

	public static void main (String[] args) throws IOException {
		AddressMap.compute();
	}
}
