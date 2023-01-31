package it.unimi.dsi.law;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class AddressMap {
    public static void main(String[] args) throws IOException {
        MutableString address = new MutableString();
        Iterable<MutableString> addressesIterator = Utils.readTSVs(Parameters.addresses.toFile(), address, null);

        File tempDir = Files.createTempDirectory(Parameters.resources, "amap_temp").toFile();
        tempDir.deleteOnExit();

        GOVMinimalPerfectHashFunction.Builder<MutableString> b = new GOVMinimalPerfectHashFunction.Builder<>();
        b.keys(addressesIterator);
        b.tempDir(tempDir);
        b.transform(TransformationStrategies.rawIso());

        BinIO.storeObject(b.build(), Parameters.addressesMap.toFile());
    }
}
