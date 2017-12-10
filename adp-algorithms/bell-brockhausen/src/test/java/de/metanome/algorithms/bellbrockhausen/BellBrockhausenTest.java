package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfo;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;
import org.jukito.JukitoRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(JukitoRunner.class)
public class BellBrockhausenTest extends IndBaseTest {

    private static final String dummyTableName = "table";

    private InclusionDependencyResultReceiver resultReceiver;
    @Inject public BellBrockhausen bellBrockhausen;

    @Before
    public void setupMocks(BellBrockhausenConfiguration config) {
        resultReceiver = getResultReciever();
        when(config.getResultReceiver()).thenReturn(resultReceiver);
        when(config.getTableName()).thenReturn(dummyTableName);
    }

    @Test
    public void givenTableInfo_when_execute_thenReturnUINDS(DataAccessObject dataAccessObject) throws AlgorithmExecutionException {
        // GIVEN
        Attribute attributeA = new Attribute(new ColumnIdentifier(dummyTableName, "a"), Range.open(1, 3));
        Attribute attributeB = new Attribute(new ColumnIdentifier(dummyTableName, "b"), Range.open(2, 4));
        Attribute attributeC = new Attribute(new ColumnIdentifier(dummyTableName, "c"), Range.open(1, 4));
        TableInfo tableInfo = new TableInfo(dummyTableName, ImmutableList.of(attributeA, attributeB, attributeC));
        InclusionDependency indAC = toInd(attributeA.getColumnIdentifier(), attributeC.getColumnIdentifier());
        InclusionDependency indBC = toInd(attributeB.getColumnIdentifier(), attributeC.getColumnIdentifier());
        ImmutableSet<InclusionDependency> validInds = ImmutableSet.of(indAC, indBC);

        when(dataAccessObject.isValidUIND(any(InclusionDependency.class)))
                .thenAnswer(invocation -> validInds.contains(invocation.<InclusionDependency>getArgument(0)));

        // WHEN
        when(dataAccessObject.getTableInfo(dummyTableName)).thenReturn(tableInfo);
        bellBrockhausen.execute();

        // THEN
        recievedAllValidInds(ImmutableList.of(attributeA, attributeB, attributeC), validInds);
    }

    private void recievedAllValidInds(ImmutableList<Attribute> attributes, ImmutableSet<InclusionDependency> validInds) {
        for (Attribute attrA : attributes) {
            for (Attribute attrB : attributes) {
                InclusionDependency ind = toInd(attrA.getColumnIdentifier(), attrB.getColumnIdentifier());
                boolean isValidInd = validInds.contains(ind);
                assertThat(resultReceiver.acceptedResult(ind)).isEqualTo(isValidInd);
            }
        }
    }
}
