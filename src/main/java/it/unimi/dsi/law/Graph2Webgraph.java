package it.unimi.dsi.law;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Block;

import com.martiansoftware.jsap.JSAPException;

import it.unimi.dsi.big.webgraph.BVGraph;

public class Graph2Webgraph {
	MultiValuedMap<Address, Address> graph;
	HashSet<Address> uniqueAddresses;

	MultiValuedMap<Integer, Integer> intGraph = new ArrayListValuedHashMap<Integer,Integer>();

	void loadMultiValuedMap(String location) throws IOException, ClassNotFoundException {
		FileInputStream load = new FileInputStream(location);
		ObjectInputStream in = new ObjectInputStream(load);
		
		Object temp = in.readObject();
		
		load.close();
		in.close();
		
		this.graph = (MultiValuedMap<Address, Address>) temp;
	}

	void loadHashSet(String location) throws IOException, ClassNotFoundException {
		FileInputStream load = new FileInputStream(location);
		ObjectInputStream in = new ObjectInputStream(load);
		
		Object temp = in.readObject();
		
		load.close();
		in.close();
		
		this.uniqueAddresses = (HashSet<Address>) temp;
	}

	void multiValuedMap2ASCII(String destination) throws IOException {
		FileOutputStream os = new FileOutputStream(destination);
		
		for (int parent: this.intGraph.keySet()) {
			CharSequence arcs = this.intGraph
				.get(parent)
				.stream()
				.filter(a -> a != null)
				.map(a -> a.toString() + " ")
				.collect(Collectors.joining());

			os.write((parent + " " + String.join(" ", arcs) + "\n").getBytes());
		}

		os.close();
		System.out.println("ASCIIGraph saved in " + destination);
	}

	void mapEdgesToIntegers() {
		// TODO: find a more efficient way to do this,maybe when building the graph
		List<Address> indexing = new ArrayList<Address>(this.uniqueAddresses);
		
		for (Address key: graph.keySet()) {
			int keyInt = indexing.indexOf(key);

			for (Address value: graph.get(key)) {
				int valueInt = indexing.indexOf(value);
				intGraph.put(keyInt, valueInt);
			}
		}
	}

	public static void main(String[] args) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, ClassNotFoundException, InstantiationException {
		Graph2Webgraph gw = new Graph2Webgraph();
		
		gw.loadMultiValuedMap(Blockchain2Graph.defaultLocation + Blockchain2Graph.edgesFilename);
		gw.loadHashSet(Blockchain2Graph.defaultLocation + Blockchain2Graph.uniqueAddressesFilename);
		gw.mapEdgesToIntegers();

		gw.multiValuedMap2ASCII(Blockchain2Graph.defaultLocation + "ascii.graph-txt");

		System.out.println("Converting to BVGraph");
		BVGraph.main(new String[] {"-g", "ASCIIGraph", Blockchain2Graph.defaultLocation + "ascii", "bitcoin"});
	}


}
