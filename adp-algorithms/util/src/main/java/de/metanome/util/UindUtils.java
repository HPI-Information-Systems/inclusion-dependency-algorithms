package de.metanome.util;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.InclusionDependency;

import static com.google.common.collect.Iterables.getOnlyElement;

public class UindUtils {

    public static ColumnIdentifier getReferenced(InclusionDependency uind) {
        return getOnlyElement(uind.getReferenced().getColumnIdentifiers());
    }

    public static ColumnIdentifier getDependant(InclusionDependency uind) {
        return getOnlyElement(uind.getDependant().getColumnIdentifiers());
    }
}
