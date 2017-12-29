package de.metanome.algorithms.sindd.sindd;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class AttributeSetCollector {

	private List<MergedListReader> readers;
	private PriorityQueue<MergedListReader> queue;

	public AttributeSetCollector(List<File> inputFiles) throws IOException {
		createReaders(inputFiles);
		initQueue();
	}

	public boolean hasNext() {
		return !queue.isEmpty();
	}

	public String[] nextValue2AttSet() throws IOException {
		String currentVal = getCurrentValue();
		Set<String> nextAttSet = collectNextAttSet(currentVal);
		return convertToOutputLine(currentVal, nextAttSet);
	}

	public Set<String> nextCluster() throws IOException {
		String currentVal = getCurrentValue();
		Set<String> nextCluster = collectNextAttSet(currentVal);
		return nextCluster;
	}

	private Set<String> collectNextAttSet(String currentVal) throws IOException {
		Set<String> nextAttSet = new HashSet<String>();
		MergedListReader currentReader = queue.poll();
		currentReader.addNextAttributesTo(nextAttSet);
		updateQueue(currentReader);
		while (!queue.isEmpty()) {
			MergedListReader nextReader = queue.peek();
			String nextVal = nextReader.getNextValue();
			if (currentVal.compareTo(nextVal) != 0) {
				break;
			} else {
				currentReader = queue.poll();
				currentReader.addNextAttributesTo(nextAttSet);
				updateQueue(currentReader);
			}
		}
		return nextAttSet;
	}

	private String getCurrentValue() {
		MergedListReader currentReader = queue.peek();
		return currentReader.getNextValue();
	}

	private String[] convertToOutputLine(String currentVal, Set<String> nextAttSet) {
		String[] outputLine = new String[nextAttSet.size() + 1];
		outputLine[0] = currentVal;
		int i = 1;
		for (String att : nextAttSet) {
			outputLine[i] = att;
			++i;
		}
		return outputLine;
	}

	public void close() throws IOException {
		for (MergedListReader reader : readers) {
			reader.close();
		}
	}

	private void updateQueue(MergedListReader reader) throws IOException {
		if (reader.hasNext()) {
			queue.add(reader);
		}
	}

	private void initQueue() throws IOException {
		queue = new PriorityQueue<MergedListReader>();
		for (MergedListReader reader : readers) {
			if (reader.hasNext()) {
				queue.add(reader);
			}
		}
	}

	private void createReaders(List<File> inFiles) throws IOException {
		readers = new ArrayList<MergedListReader>(inFiles.size());
		for (File inFile : inFiles) {
			MergedListReader reader = new MergedListReader(inFile);
			readers.add(reader);
		}
	}
}
