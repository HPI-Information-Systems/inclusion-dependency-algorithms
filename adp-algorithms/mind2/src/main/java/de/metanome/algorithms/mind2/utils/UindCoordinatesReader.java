package de.metanome.algorithms.mind2.utils;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithms.mind2.model.UindCoordinates;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

import java.io.BufferedReader;
import java.io.IOException;

public class UindCoordinatesReader implements AutoCloseable {

    private final int uindId;
    private final BufferedReader reader;
    private IntList currentRhsIndices = IntLists.EMPTY_LIST;
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
