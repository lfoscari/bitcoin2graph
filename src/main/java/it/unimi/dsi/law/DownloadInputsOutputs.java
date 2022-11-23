package it.unimi.dsi.law;

import com.opencsv.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.CleanedBitcoinColumn.TRANSACTION_HASH;

public class DownloadInputsOutputs {
	public static final List<Integer> INPUTS_IMPORTANT = List.of(SPENDING_TRANSACTION_HASH, INDEX, RECIPIENT);
	public static final List<Integer> OUTPUTS_IMPORTANT = List.of(TRANSACTION_HASH, INDEX, RECIPIENT);

	public static final Integer INPUTS_AMOUNT = 10;
	public static final Integer OUTPUTS_AMOUNT = 10;

	private final ProgressLogger progress;

	public DownloadInputsOutputs () {
		this(null);
	}

	public DownloadInputsOutputs (ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(DownloadInputsOutputs.class);
			progress = new ProgressLogger(logger);
		}

		this.progress = progress;
	}

	public void run () throws IOException {
		this.download(Parameters.inputsUrlsFilename.toFile(), INPUTS_AMOUNT);
		this.download(Parameters.outputsUrlsFilename.toFile(), OUTPUTS_AMOUNT);

		this.computeAddressMap();
		this.createBloomFilters();
	}

	private void download (File urls, int limit) throws IOException {

		Parameters.inputsDirectory.toFile().mkdir();
		Parameters.outputsDirectory.toFile().mkdir();

		Path tempDir = Files.createTempDirectory(Parameters.resources, "download-");
		tempDir.toFile().deleteOnExit();

		FileReader reader = new FileReader(urls);
		List<String> toDownload = new BufferedReader(reader).lines().toList();

		if (limit >= 0) {
			this.progress.start("Downloading and unpacking first " + INPUTS_AMOUNT + " lines in " + urls + "...");
			toDownload = toDownload.subList(0, limit);
		} else {
			this.progress.start("Downloading and unpacking " + urls + "...");
		}

		FileWriter addressesFile = new FileWriter(Parameters.addressesTSV.toFile());

		for (String s : toDownload) {
			URL url = new URL(s);

			String filename = s.substring(s.lastIndexOf("/") + 1, s.indexOf(".gz?"));
			Path tempPath = tempDir.resolve(filename);

			try (GZIPInputStream gzip = new GZIPInputStream(url.openStream());
				 ReadableByteChannel rbc = Channels.newChannel(gzip);
				 FileOutputStream fos = new FileOutputStream(tempPath.toFile())) {

				fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
				boolean contentful = this.parseTSV(tempPath.toFile(), addressesFile);

				if (contentful) {
					this.progress.lightUpdate();
				}
			}

			tempPath.toFile().delete();
		}

		addressesFile.close();
		reader.close();

		this.progress.stop();
	}

	public boolean parseTSV (File tsv, FileWriter addressesFile) throws IOException {
		List<Integer> important;
		Path destinationPath;

		if (tsv.toString().contains("input")) {
			important = INPUTS_IMPORTANT;
			destinationPath = Parameters.inputsDirectory.resolve(tsv.getName());
		} else {
			important = OUTPUTS_IMPORTANT;
			destinationPath = Parameters.outputsDirectory.resolve(tsv.getName());
		}

		List<String[]> content = Utils.readTSV(tsv)
				.stream()
				.filter(line -> line[IS_FROM_COINBASE].equals("0"))
				.map(line -> important.stream().map(i -> line[i]).toList().toArray(String[]::new))
				.toList();

		if (content.size() <= 1) {
			return false;
		}

		this.saveTSV(content, destinationPath);
		this.saveAddresses(content, addressesFile);

		return true;
	}

	private void saveTSV (List<String[]> content, Path destinationPath) throws IOException {
		try (FileWriter destinationWriter = new FileWriter(destinationPath.toString());
			 CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n")) {
			tsvWriter.writeAll(content, false);
		}
	}

	private void saveAddresses (List<String[]> content, FileWriter addressesFile) throws IOException {
		Set<String> addresses = content.stream().map(line -> line[Parameters.CleanedBitcoinColumn.RECIPIENT]).collect(Collectors.toSet());

		for (String address : addresses) {
			addressesFile.write(address + "\n");
		}
	}

	private void computeAddressMap () throws IOException {
		this.progress.start("Computing address-to-long map...");

		if (!Parameters.addressesTSV.toFile().exists()) {
			throw new NoSuchFileException("Couldn't find addresses tsv");
		}

		long count = 0;
		Object2LongFunction<String> addressLong = new Object2LongArrayMap<>();

		for (String address : Files.readAllLines(Parameters.addressesTSV)) {
			addressLong.put(address, count++);
		}

		BinIO.storeObject(addressLong, Parameters.addressLongMap.toFile());

		this.progress.stop("Map stored in " + Parameters.addressLongMap);
	}

	public void createBloomFilters () throws IOException {
		this.progress.start("Creating bloom filters...");

		Parameters.filtersDirectory.toFile().mkdir();
		File[] outputs = Parameters.outputsDirectory.toFile().listFiles((d, f) -> f.endsWith("tsv"));

		if (outputs == null) {
			throw new FileNotFoundException("No outputs found!");
		}

		for (File output : outputs) {
			this.bloom(output);
		}

		this.progress.stop("Filters saved in " + Parameters.filtersDirectory);
	}

	public void bloom (File output) throws IOException {
		BloomFilter<CharSequence> transactionFilter = BloomFilter.create(1000, BloomFilter.STRING_FUNNEL);

		FileReader originalReader = new FileReader(output);
		CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
		CSVReader tsvReader = new CSVReaderBuilder(originalReader).withCSVParser(tsvParser).build();

		tsvReader.readNext(); // header
		tsvReader.iterator().forEachRemaining(line -> transactionFilter.add(line[TRANSACTION_HASH].getBytes()));

		BinIO.storeObject(transactionFilter, Parameters.filtersDirectory.resolve(output.getName()).toFile());
	}

	public static void main (String[] args) throws IOException {
		new DownloadInputsOutputs().run();
	}
}
