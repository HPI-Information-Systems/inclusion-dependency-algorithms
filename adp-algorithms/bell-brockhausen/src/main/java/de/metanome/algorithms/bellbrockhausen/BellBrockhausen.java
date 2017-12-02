package de.metanome.algorithms.bellbrockhausen;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfo;
import de.metanome.algorithms.bellbrockhausen.accessors.PostgresTableInfoFactory;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfoFactory;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;

import java.util.ArrayList;
import java.util.List;

public class BellBrockhausen {

    private final BellBrockhausenConfiguration configuration;
    private final TableInfoFactory tableInfoFactory;

    public BellBrockhausen(final BellBrockhausenConfiguration configuration) {
        this.configuration = configuration;
        this.tableInfoFactory = new PostgresTableInfoFactory(); // TODO: Inject database specific factory
    }

    public void execute() throws AlgorithmExecutionException {
        TableInfo tableInfo = tableInfoFactory.getTableInfo(
                configuration.getConnectionGenerator(), configuration.getTableName());
        List<InclusionDependency> candidates = generateCandidates(tableInfo);
    }

    private List<InclusionDependency> generateCandidates(TableInfo tableInfo) {
        List<InclusionDependency> candidates = new ArrayList<>();
        for (Attribute attributeA : tableInfo.getAttributes()) {
            for (Attribute attributeB : tableInfo.getAttributes()) {
                if (attributeA.equals(attributeB)) continue;
                if (attributeA.getValueRange().encloses(attributeB.getValueRange())) {
                    candidates.add(new InclusionDependency(
                            new ColumnPermutation(attributeB.getColumnIdentifier()),
                            new ColumnPermutation(attributeA.getColumnIdentifier())));
                }
                if (attributeB.getValueRange().encloses(attributeA.getValueRange())) {
                    candidates.add(new InclusionDependency(
                            new ColumnPermutation(attributeA.getColumnIdentifier()),
                            new ColumnPermutation(attributeB.getColumnIdentifier())));
                }
            }
        }
        return candidates;
    }
}
