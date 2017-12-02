package de.metanome.algorithms.bellbrockhausen.models;

import lombok.Data;

@Data
public class Attribute {

    private final String name;
    private final int minValue;
    private final int maxValue;
}
