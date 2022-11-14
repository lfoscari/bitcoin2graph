package it.unimi.dsi.law;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.apache.commons.math3.analysis.function.Add;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;

public class Blockchain2ScatteredArcsASCIIGraph {
	public static void main (String[] args) throws RocksDBException, IOException, InterruptedException, ExecutionException {
		NetworkParameters np = new MainNetParams();
		Context c = new Context(np);
		Context.propagate(c);

		(new File(Parameters.resources + "ScatteredArcsASCIIGraph/")).mkdir();

		Logger logger = LoggerFactory.getLogger(Blockchain2ScatteredArcsASCIIGraph.class);
		ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "blocks");

		List<File> blockFiles = getBlockFiles(Parameters.resources + "blocks");

		// try (AddressConversion ac = new AddressConversion(np, progress)) {
		//     ac.addAddresses(blockFiles);
		// }

		try (AddressConversion ac = new AddressConversion(np, progress, true);
			CustomBlockchainIterator it = new CustomBlockchainIterator(blockFiles, ac, np, progress)) {
			it.populateMappings();
			it.completeMappings();
		}

		// Path tempDirectory = Files.createTempDirectory(Path.of(Parameters.resources), "scatteredgraph-");
		// ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(it.iterator(), false, false, 1000, tempDirectory.toFile(), progress);
		// BVGraph.store(graph, Parameters.basename, progress);
		//
		// progress.stop("Results saved in " + Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename);
		//
		// progress.start("Creating address-to-node map");
		// Address2Node.saveAddress2Node(graph.ids, addressConversion);
		// progress.stop();

		progress.done();
	}

	public static List<File> getBlockFiles (String blocksDirName) {
		File blocksDir = new File(blocksDirName);
		List<File> list = new ArrayList<>();
		for (int i = 0; true; i++) {
			File file = new File(blocksDir, String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists()) {
                break;
            }
			list.add(file);
		}
		return list;
	}

	public static Object extract(Object o, String fieldName) {
		try {
			Field f = o.getClass().getDeclaredField(fieldName);
			f.setAccessible(true);
			return f.get(o);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
