package de.metanome.algorithms.bellbrockhausen.accessors;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;
import lombok.Data;

@Data
public class TableInfo {

    private final String tableName;
    private final ImmutableList<Attribute> attributes;
}
