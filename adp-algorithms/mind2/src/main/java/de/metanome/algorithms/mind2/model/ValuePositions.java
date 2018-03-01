package de.metanome.algorithms.mind2.model;

import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class ValuePositions {
    private final List<Integer> positionsA;
    private final List<Integer> positionsB;

    public ValuePositions(List<Integer> positionsA, List<Integer> positionsB) {
        this.positionsA = positionsA;
        this.positionsB = positionsB;
    }

    public  ValuePositions(int posA, int posB) {
        positionsA = Collections.singletonList(posA);
        positionsB = Collections.singletonList(posB);
    }
}
