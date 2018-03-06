package de.metanome.algorithms.mind2.utils;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.model.UindCoordinates;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

public class UindCoordinatesReader implements AutoCloseable {

    private final int uindId;
    private final BufferedReader reader;
    private List<Integer> currentRhsIndices = ImmutableList.of();
    private String line;
    private UindCoordinates current;

    public UindCoordinatesReader(int uindId, BufferedReader reader) throws AlgorithmExecutionException {
        this.uindId = uindId;
        this.reader = reader;
        this.line = getNextLine();
        next();
    }

    public boolean hasNext() {
        return line != null;
    }

    public UindCoordinates next() throws AlgorithmExecutionException {
        UindCoordinates result = current;
        current = UindCoordinates.fromLine(uindId, line, currentRhsIndices);
        currentRhsIndices = current.getRhsIndices();
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

    @Override
    public void close() throws AlgorithmExecutionException {
        try {
            reader.close();
        } catch (IOException e) {
            throw new AlgorithmExecutionException("Error closing BufferedReader", e);
        }
    }
}
