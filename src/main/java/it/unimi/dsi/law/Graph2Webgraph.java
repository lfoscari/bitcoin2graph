package it.unimi.dsi.law;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MultiValuedMap;

import com.martiansoftware.jsap.JSAPException;

import it.unimi.dsi.big.webgraph.BVGraph;

public class Graph2Webgraph {
	MultiValuedMap<Long, Long> graph;

	void loadMultiValuedMap(String location) throws IOException, ClassNotFoundException {
		FileInputStream load = new FileInputStream(location);
		ObjectInputStream in = new ObjectInputStream(load);
		
		Object temp = in.readObject();
		
		load.close();
		in.close();
		
		this.graph = (MultiValuedMap<Long, Long>) temp;
	}

	void multiValuedMap2ASCII(String destination, long totalNodes) throws IOException {
		FileOutputStream os = new FileOutputStream(destination);
		
		os.write((totalNodes + "\n").getBytes());

		for (long parent: this.graph.keySet()) {
			CharSequence arcs = this.graph
				.get(parent)
				.stream()
				.filter(a -> a != null)
				.map(a -> a.toString() + " ")
				.collect(Collectors.joining());

			os.write((parent + " " + String.join(" ", arcs).stripTrailing() + "\n").getBytes());
		}

		os.close();
		System.out.println("ASCIIGraph saved in " + destination);
	}

	public static void main(String[] args) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, ClassNotFoundException, InstantiationException {
		Graph2Webgraph gw = new Graph2Webgraph();

		long totalNodes = Blockchain2Graph.progression - 1;
		
		gw.loadMultiValuedMap(Blockchain2Graph.defaultLocation + Blockchain2Graph.edgesFilename);
		gw.multiValuedMap2ASCII(Blockchain2Graph.defaultLocation + "ascii.graph-txt", totalNodes);

		System.out.println("Converting to BVGraph");
		BVGraph.main(new String[] {"-g", "ASCIIGraph", Blockchain2Graph.defaultLocation + "ascii", "bitcoin"});
	}
}
