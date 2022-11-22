package it.unimi.dsi.law;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class DownloadInputsOutputs {
    public static void main(String[] args) {
        File inputUrls = Path.of(Parameters.resources, "input-urls.txt").toFile();
        download(inputUrls, "originals", 100);

        File outputUrls = Path.of(Parameters.resources, "output-urls.txt").toFile();
        download(outputUrls, "originals", 100);

        System.out.println("Remember to untar the files!");
    }

    private static void download(File urls, String destinationDir, int limit) {
        Path.of(Parameters.resources, destinationDir).toFile().mkdir();

        try (FileReader reader = new FileReader(urls)) {
            BufferedReader buf = new BufferedReader(reader);
            for (String s : buf.lines().toList().subList(0, limit)) {
                URL url = new URL(s);

                String filename = s.substring(s.lastIndexOf("/") + 1, s.indexOf("?"));
                Path fullPath = Path.of(Parameters.resources, destinationDir, filename);

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
