package de.metanome.algorithms.sindd.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class FileUtil {

	public static CSVWriter createWriter(File outFile) throws IOException {
		CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(outFile)), ',', '"');
		return writer;
	}

	public static CSVReader createReader(File inFile) throws IOException {
		CSVReader reader = new CSVReader(new BufferedReader(new FileReader(inFile)), ',', '"');
		return reader;
	}
}
