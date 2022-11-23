package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

public class AddressToLong {
	public static void main (String[] args) throws IOException {
		File addressesFile = Parameters.addressesTSV.toFile();

		if (!addressesFile.exists()) {
			throw new NoSuchFileException("Couldn't find addresses tsv");
		}

		Object2LongFunction<String> addressLong = new Object2LongArrayMap<>();
		long count = 0;

		for (String[] line: Utils.readTSV(addressesFile)) {
			addressLong.put(line[0], count++);
		}

		File destinationFile = Parameters.addressLongMap.toFile();
		BinIO.storeObject(addressLong, destinationFile);
	}
}
