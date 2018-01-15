package de.metanome.algorithms.mind2.utils;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.model.UindCoordinates;

import java.io.BufferedReader;
import java.io.IOException;

public class UindCoordinatesReader {

    private final InclusionDependency uind;
    private final BufferedReader reader;
    private String line;
    private UindCoordinates current;

    public UindCoordinatesReader(InclusionDependency uind, BufferedReader reader) throws AlgorithmExecutionException {
        this.uind = uind;
        this.reader = reader;
        this.line = getNextLine();
        next();
    }

    public boolean hasNext() {
        return line != null;
    }

    public UindCoordinates next() throws AlgorithmExecutionException {
        UindCoordinates result = current;
        current = UindCoordinates.fromLine(uind, line);
        line = getNextLine();
        return result;
    }

    public UindCoordinates current() {
        return current;
    }

    private String getNextLine() throws AlgorithmExecutionException {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new AlgorithmExecutionException("Error reading uind coordinate from file.", e);
        }
    }
}
