package de.metanome.algorithms.mind2.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.ResultSetIterator;
import lombok.Builder;
import lombok.Data;

import java.sql.SQLException;

@Data
@Builder
public class Mind2Configuration {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final ImmutableList<TableInputGenerator> inputGenerators;
    private final ImmutableSet<InclusionDependency> unaryInds;
    private final FileGenerator tempFileGenerator;
    private final String indexColumn;

    public RelationalInput getSortedRelationalInput(TableInputGenerator inputGenerator, ColumnIdentifier column)
            throws AlgorithmExecutionException {
        try {
            return new ResultSetIterator(inputGenerator.sortBy(column.getColumnIdentifier(), false));
        } catch (SQLException e) {
            throw new AlgorithmExecutionException("Could not construct database input", e);
        }
    }
}
