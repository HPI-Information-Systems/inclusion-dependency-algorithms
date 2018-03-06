package de.metanome.algorithms.mind2.model;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.results.InclusionDependency;
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

    public static UindCoordinates fromLine(int uindId, String data, List<Integer> currentRhsIndices) {
        ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on(FIELD_SEPERATOR).trimResults().split(data));
        int lhsIndex = Integer.valueOf(parts.get(0));
        if (parts.get(1).equals(SAME_RHS_INDICATOR)) {
            return new UindCoordinates(uindId, lhsIndex, currentRhsIndices);
        }
        ImmutableList<Integer> rhsIndices = StreamSupport.stream(
                Splitter.on(ELEM_SEPERATOR).trimResults().split(parts.get(1)).spliterator(), false)
                .map(Integer::valueOf).collect(toImmutableList());
        return new UindCoordinates(uindId, lhsIndex, rhsIndices);
    }

    private final int uindId;
    private final Integer lhsIndex;
    private final List<Integer> rhsIndices;

    public UindCoordinates(int uindId, Integer lhsIndex, List<Integer> rhsIndices) {
        this.uindId = uindId;
        this.lhsIndex = lhsIndex;
        this.rhsIndices = rhsIndices;
    }

    public static String toLine(int lhsIndex, List<Integer> rhsIndices) {
        return lhsIndex + FIELD_SEPERATOR + Joiner.on(ELEM_SEPERATOR).join(rhsIndices.stream().sorted().collect(toList()));
    }

    public static String toLine(int lhsIndex) {
        return lhsIndex + FIELD_SEPERATOR + SAME_RHS_INDICATOR;
    }
}
