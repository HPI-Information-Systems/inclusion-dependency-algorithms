package de.metanome.algorithms.mind2.utils;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithms.mind2.model.AttributeValuePosition;
import de.metanome.util.CheckedAbstractIterator;

import java.util.List;

import static java.lang.String.format;

public class AttributeIterator extends CheckedAbstractIterator<AttributeValuePosition> implements AutoCloseable {

    private final RelationalInput relationalInput;
    private final int columnIndex;
    private final int indexColumnIndex;

    public AttributeIterator(RelationalInput relationalInput, ColumnIdentifier columnIdentifier, String indexColumnIdentifier)
            throws AlgorithmConfigurationException {
        this.relationalInput = relationalInput;
        columnIndex = relationalInput.columnNames().indexOf(columnIdentifier.getColumnIdentifier());
        indexColumnIndex = relationalInput.columnNames().indexOf(indexColumnIdentifier);
        if (columnIndex == -1 || indexColumnIndex == -1) {
            throw new AlgorithmConfigurationException(
                    format("Invalid column %s or %s for relational input %s with columns %s",
                            columnIdentifier, indexColumnIndex, relationalInput, relationalInput.columnNames()));
        }
    }

    protected AttributeValuePosition computeNext() throws InputIterationException {
        while (relationalInput.hasNext()) {
            List<String> row = relationalInput.next();
            String value = row.get(columnIndex);
            int index = Integer.valueOf(row.get(indexColumnIndex));
            if (value != null ) {
                return new AttributeValuePosition(value, index);
            }
        }
        return endOfData();
    }

    public void close() throws InputGenerationException {
        try {
            relationalInput.close();
        } catch (Exception e) {
            throw new InputGenerationException(format("Error closing relational input %s", relationalInput), e);
        }
    }
}
