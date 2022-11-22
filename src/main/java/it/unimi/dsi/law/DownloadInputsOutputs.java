package it.unimi.dsi.law;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DownloadInputsOutputs {
	public static void main (String[] args) {
		File inputUrls = Path.of(Parameters.resources, Parameters.inputsUrlsFilename).toFile();
		download(inputUrls, -1);

		File outputUrls = Path.of(Parameters.resources, Parameters.outputsUrlsFilename).toFile();
		download(outputUrls, -1);

		System.out.println("Remember to untar the files!\n\t> find . -name '*.tar.gz' -exec tar -xzf {} \\;");
	}

	private static void download (File urls, int limit) {
		Path.of(Parameters.resources, Parameters.originalsDirectory).toFile().mkdir();

		try (FileReader reader = new FileReader(urls)) {
			List<String> toDownload = new BufferedReader(reader).lines().toList();

			if (limit >= 0) {
				toDownload = toDownload.subList(0, limit);
			}

			for (String s : toDownload) {
				URL url = new URL(s);

				String filename = s.substring(s.lastIndexOf("/") + 1, s.indexOf("?"));
				Path fullPath = Path.of(Parameters.resources, Parameters.originalsDirectory, filename);

				try (ReadableByteChannel rbc = Channels.newChannel(url.openStream());
					 FileOutputStream fos = new FileOutputStream(fullPath.toFile())) {
					fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
					System.out.println("Saved " + filename);
				}

				Thread.sleep(100);
			}
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
