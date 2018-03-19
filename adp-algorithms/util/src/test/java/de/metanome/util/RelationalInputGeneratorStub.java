package de.metanome.util;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import lombok.Builder;
import lombok.Singular;

@Builder
public class RelationalInputGeneratorStub implements RelationalInputGenerator {

    public final String relationName;
    @Singular
    public final ImmutableList<String> columnNames;
    @Singular
    public final ImmutableList<Row> rows;

    public RelationalInputGeneratorStub(String relationName, ImmutableList<String> columnNames, ImmutableList<Row> rows) {
        this.relationName = relationName;
        this.columnNames = columnNames;
        this.rows = rows;
    }

    @Override
    public RelationalInput generateNewCopy() throws InputGenerationException, AlgorithmConfigurationException {
        return new RelationalInputStub(relationName, columnNames, rows);
    }

    @Override
    public void close() throws Exception {
        // no-op
    }
}
