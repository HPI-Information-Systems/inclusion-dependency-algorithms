package de.metanome.algorithms.mind2.model;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class ValuePositions {
    private final IntList positionsA;
    private final IntList positionsB;

    public ValuePositions(IntList positionsA, IntList positionsB) {
        this.positionsA = positionsA;
        this.positionsB = positionsB;
    }

    public  ValuePositions(int posA, int posB) {
        positionsA = IntLists.singleton(posA);
        positionsB = IntLists.singleton(posB);
    }
}
