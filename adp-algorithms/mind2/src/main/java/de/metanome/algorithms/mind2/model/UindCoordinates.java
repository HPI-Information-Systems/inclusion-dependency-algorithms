package de.metanome.algorithms.mind2.model;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.results.InclusionDependency;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static de.metanome.util.Collectors.toImmutableList;
import static java.util.stream.Collectors.toList;

@Data
public class UindCoordinates {

    public static final String FIELD_SEPERATOR = ";";
    public static final String ELEM_SEPERATOR = ",";
    public static final String SAME_RHS_INDICATOR = "\\N";

    public static UindCoordinates fromLine(Integer uindId, String data, IntList currentRhsIndices) {
        ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on(FIELD_SEPERATOR).trimResults().split(data));
        int lhsIndex = Integer.valueOf(parts.get(0));
        if (parts.get(1).equals(SAME_RHS_INDICATOR)) {
            return new UindCoordinates(uindId, lhsIndex, currentRhsIndices);
        }
        IntList rhsIndices = new IntArrayList();
        Iterable<String> indices = Splitter.on(ELEM_SEPERATOR).trimResults().split(parts.get(1));
        for (String index : indices) {
            rhsIndices.add(Integer.parseInt(index));
        }
        return new UindCoordinates(uindId, lhsIndex, rhsIndices);
    }

    private final int uindId;
    private final int lhsIndex;
    private final IntList rhsIndices;

    public UindCoordinates(int uindId, int lhsIndex, IntList rhsIndices) {
        this.uindId = uindId;
        this.lhsIndex = lhsIndex;
        this.rhsIndices = rhsIndices;
    }

    public static String toLine(Integer lhsIndex, List<Integer> rhsIndices) {
        return lhsIndex + FIELD_SEPERATOR + Joiner.on(ELEM_SEPERATOR).join(rhsIndices.stream().sorted().collect(toList()));
    }

    public static String toLine(Integer lhsIndex) {
        return lhsIndex + FIELD_SEPERATOR + SAME_RHS_INDICATOR;
    }
}
