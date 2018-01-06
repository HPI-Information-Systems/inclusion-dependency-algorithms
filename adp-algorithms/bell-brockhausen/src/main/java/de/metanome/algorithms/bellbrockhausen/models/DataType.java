package de.metanome.algorithms.bellbrockhausen.models;

import com.google.common.base.Enums;

public enum DataType {
    TEXT,
    INTEGER;

    public static DataType fromString(String str) {
        return Enums.getIfPresent(DataType.class, str)
                .or(DataType.TEXT);
    }
}
