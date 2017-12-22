package de.metanome.algorithms.mind2.utils;

import de.metanome.algorithm_integration.results.InclusionDependency;

import java.io.BufferedReader;
import java.io.IOException;

public class UindCoordinatesReader {

    private final InclusionDependency uind;
    private final BufferedReader reader;
    private UindCoordinates nextCoordinates;

    public UindCoordinatesReader(InclusionDependency uind, BufferedReader reader) throws IOException {
        this.uind = uind;
        this.reader = reader;
        nextCoordinates = next();
    }

    public boolean hasNext() {
        return nextCoordinates == null;
    }

    public UindCoordinates next() throws IOException {
        UindCoordinates result = nextCoordinates;
        nextCoordinates = UindCoordinates.fromLine(uind, reader.readLine());
        return result;
    }

    public UindCoordinates peek() {
        return nextCoordinates;
    }
}
