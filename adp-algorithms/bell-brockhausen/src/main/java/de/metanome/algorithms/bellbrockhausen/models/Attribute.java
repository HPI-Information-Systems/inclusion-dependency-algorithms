package de.metanome.algorithms.bellbrockhausen.models;

import com.google.common.collect.Range;
import de.metanome.algorithm_integration.ColumnIdentifier;
import lombok.Data;

@Data
public class Attribute {

    private final ColumnIdentifier columnIdentifier;
    private final Range<Comparable> valueRange;
}
