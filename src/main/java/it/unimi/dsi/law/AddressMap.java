package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static it.unimi.dsi.law.Parameters.*;

public class AddressMap {
    void compute() throws IOException {
        Object2IntFunction<String> addressMap = new Object2IntOpenHashMap<>();
        int count = 1;

        Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
        ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "addresses");
        progress.start("Building address to long map");

        for (String[] addressLine : Utils.readTSVs(addressFile.toFile())) {
            if (!addressMap.containsKey(addressLine[0])) {
                addressMap.put(addressLine[0], count);
                count = Math.addExact(count, 1);
                progress.lightUpdate();
            }
        }

        progress.start("Saving address map (total " + count + " addresses)");
        BinIO.storeObject(addressMap, addressMapLocation.toFile());
        progress.stop("Map saved in " + addressMapLocation);
    }

    public static void main (String[] args) throws IOException {
        new AddressMap().compute();
    }
}