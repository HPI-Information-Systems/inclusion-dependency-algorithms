package de.metanome.algorithms.sindd.sindd;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.opencsv.CSVWriter;

import de.metanome.algorithms.sindd.sindd.Partition;
import de.metanome.algorithms.sindd.util.FileUtil;

public class Merger {

	private static final String MERGED_FILE_NAME_PREFIX = "merged-";

	private File currentInputDir;
	private File currentOutputDir;
	private int k;
	private int mergedFileCounter;

	public Merger(int openFileNumber) {
		k = openFileNumber;
	}

	public void merge(Partition partition) throws IOException, InterruptedException {
		currentInputDir = partition.getFirstDir();
		currentOutputDir = partition.getSecondDir();
		resetMergedFileCounter();
		merge();
	}

	private void merge() throws IOException, InterruptedException {
		while (hasFilesToMerge()) {
			merge(currentInputDir, currentOutputDir);
			swapDirs();
		}
	}

	private synchronized void merge(File inDir, File outDir) throws IOException, InterruptedException {

		List<File> remainingInputFiles = new ArrayList<File>(Arrays.asList(inDir.listFiles()));
		List<File> processedFiles = new ArrayList<File>();
		int outDirFileNr = countFiles(outDir);

		while (remainingInputFiles.size() != 0 && (remainingInputFiles.size() + outDirFileNr > k)) {
			List<File> nextInputFiles = getNextInputFiles(remainingInputFiles, processedFiles);
			File outputFile = new File(outDir + File.separator + getMergedFileName());
			merge(nextInputFiles, outputFile);
		}

		removeProcessedFiles(processedFiles);
	}

	private synchronized void merge(List<File> inputFiles, File outputFile) throws IOException {
		AttributeSetCollector collector = null;
		CSVWriter writer = null;
		try {
			collector = new AttributeSetCollector(inputFiles);
			writer = FileUtil.createWriter(outputFile);
			while (collector.hasNext()) {
				String[] nextList = collector.nextValue2AttSet();
				writer.writeNext(nextList);
			}
		} finally {
			if (collector != null) {
				collector.close();
			}
			if (writer != null) {
				writer.close();
			}
		}
	}

	private List<File> getNextInputFiles(List<File> remainingInputFile, List<File> processedFiles) {
		int nextInputFileNr = Math.min(k, remainingInputFile.size());
		List<File> nextInputFiles = new ArrayList<File>(nextInputFileNr);
		Iterator<File> iter = remainingInputFile.iterator();
		while (iter.hasNext() && nextInputFiles.size() < nextInputFileNr) {
			File nextInFile = iter.next();
			nextInputFiles.add(nextInFile);
			processedFiles.add(nextInFile);
			iter.remove();
		}
		return nextInputFiles;
	}

	private boolean hasFilesToMerge() {
		int fileNumberInInDir = countFiles(currentInputDir);
		int fileNumberInOutDir = countFiles(currentOutputDir);
		if (fileNumberInInDir + fileNumberInOutDir > k) {
			return true;
		}
		return false;
	}

	private int countFiles(File dir) {
		return dir.list().length;
	}

	private void removeProcessedFiles(List<File> processedFiles) {
		for (File file : processedFiles) {
			file.delete();
		}
	}

	private void swapDirs() {
		File temp = currentInputDir;
		currentInputDir = currentOutputDir;
		currentOutputDir = temp;
	}

	private String getMergedFileName() {
		return MERGED_FILE_NAME_PREFIX + String.valueOf(++mergedFileCounter);
	}

	private void resetMergedFileCounter() {
		mergedFileCounter = 0;
	}
}
