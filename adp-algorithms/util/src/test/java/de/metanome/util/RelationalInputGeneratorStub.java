package de.metanome.util;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import lombok.Builder;

@Builder
public class RelationalInputGeneratorStub implements RelationalInputGenerator {

    public final ImmutableList<String> columnNames;
    public final ImmutableList<Row> rows;

    public RelationalInputGeneratorStub(ImmutableList<String> columnNames, ImmutableList<Row> rows) {
        this.columnNames = columnNames;
        this.rows = rows;
    }

    @Override
    public RelationalInput generateNewCopy() throws InputGenerationException, AlgorithmConfigurationException {
        return new RelationalInputStub(columnNames, rows);
    }

    @Override
    public void close() throws Exception {
        // no-op
    }
}
