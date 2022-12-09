package it.unimi.dsi.law;

import com.google.common.primitives.Chars;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.chars.CharHash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.law.Utils.TSVDirectoryLineReader;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;

public class AddressMap {
	static void compute() throws IOException {
		Object2LongFunction<CharSequence> addressMap = new Object2LongOpenHashMap<>();
		long count = 0;

		Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
		ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "addresses");
		progress.displayLocalSpeed = true;
		progress.start("Building address to long map");

		TSVDirectoryLineReader addresses = new TSVDirectoryLineReader(addressesFile.toFile());

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
		BinIO.storeObject(addressMap, addressesMapFile.toFile());
		progress.stop("Map saved in " + addressesMapFile);
	}

	public static void main (String[] args) throws IOException {
		AddressMap.compute();
	}
}
