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
    private int rowIndex = 0; // 0 for starting at 1 to map row index of paper

    public AttributeIterator(RelationalInput relationalInput, ColumnIdentifier columnIdentifier)
            throws AlgorithmConfigurationException {
        this.relationalInput = relationalInput;
        columnIndex = relationalInput.columnNames().indexOf(columnIdentifier.getColumnIdentifier());
        if (columnIndex == -1) {
            throw new AlgorithmConfigurationException(
                    format("Invalid column %s for relational input %s", columnIdentifier, relationalInput));
        }
    }

    protected AttributeValuePosition computeNext() throws InputIterationException {
        while (relationalInput.hasNext()) {
            List<String> row = relationalInput.next();
            rowIndex++;
            String value = row.get(columnIndex);
            if (value != null) {
                AttributeValuePosition attrValue = new AttributeValuePosition(value, rowIndex);
                return attrValue;
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
