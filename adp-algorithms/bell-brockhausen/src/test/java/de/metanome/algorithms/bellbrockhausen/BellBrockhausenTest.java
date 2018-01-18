package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfo;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;
import de.metanome.algorithms.bellbrockhausen.models.DataType;
import de.metanome.util.InclusionDependencyResultReceiverStub;
import org.jukito.JukitoRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static de.metanome.algorithms.bellbrockhausen.models.DataType.INTEGER;
import static de.metanome.algorithms.bellbrockhausen.models.DataType.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(JukitoRunner.class)
public class BellBrockhausenTest {

    private static final String TABLE_NAME = "table";

    private InclusionDependencyResultReceiverStub resultReceiver;
    @Inject private BellBrockhausen bellBrockhausen;

    @Before
    public void setupMocks(BellBrockhausenConfiguration config) {
        resultReceiver = new InclusionDependencyResultReceiverStub();
        when(config.getResultReceiver()).thenReturn(resultReceiver);
        when(config.getTableNames()).thenReturn(ImmutableList.of(TABLE_NAME));
    }

    @Test
    public void testTableWithTwoInds(DataAccessObject dataAccessObject) throws AlgorithmExecutionException {
        // GIVEN
        Attribute attributeA = new Attribute(new ColumnIdentifier(TABLE_NAME, "a"), Range.closed(1, 3), INTEGER);
        Attribute attributeB = new Attribute(new ColumnIdentifier(TABLE_NAME, "b"), Range.closed(2, 4), INTEGER);
        Attribute attributeC = new Attribute(new ColumnIdentifier(TABLE_NAME, "c"), Range.closed(1, 4), INTEGER);
        ImmutableList<Attribute> attributes = ImmutableList.of(attributeA, attributeB, attributeC);
        TableInfo tableInfo = new TableInfo(TABLE_NAME, attributes);
        InclusionDependency indAC = toInd(attributeA.getColumnIdentifier(), attributeC.getColumnIdentifier());
        InclusionDependency indBC = toInd(attributeB.getColumnIdentifier(), attributeC.getColumnIdentifier());
        ImmutableSet<InclusionDependency> validInds = ImmutableSet.of(indAC, indBC);

        when(dataAccessObject.isValidUIND(any(InclusionDependency.class)))
                .thenAnswer(invocation -> validInds.contains(invocation.<InclusionDependency>getArgument(0)));

        // WHEN
        when(dataAccessObject.getTableInfo(TABLE_NAME)).thenReturn(tableInfo);
        bellBrockhausen.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(validInds));
    }

    @Test
    public void testTableWithNoInds(DataAccessObject dataAccessObject) throws AlgorithmExecutionException {
        // GIVEN
        Attribute attributeA = new Attribute(new ColumnIdentifier(TABLE_NAME, "a"), Range.closed(1, 3), INTEGER);
        Attribute attributeB = new Attribute(new ColumnIdentifier(TABLE_NAME, "b"), Range.closed(2, 4), INTEGER);
        Attribute attributeC = new Attribute(new ColumnIdentifier(TABLE_NAME, "c"), Range.closed(1, 4), INTEGER);
        TableInfo tableInfo = new TableInfo(TABLE_NAME, ImmutableList.of(attributeA, attributeB, attributeC));
        ImmutableSet<InclusionDependency> validInds = ImmutableSet.of();

        when(dataAccessObject.isValidUIND(any(InclusionDependency.class))).thenReturn(false);

        // WHEN
        when(dataAccessObject.getTableInfo(TABLE_NAME)).thenReturn(tableInfo);
        bellBrockhausen.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(validInds));
    }

    @Test
    public void testTableWithTransitiveInds(DataAccessObject dataAccessObject) throws AlgorithmExecutionException {
        // GIVEN
        Attribute attributeA = new Attribute(new ColumnIdentifier(TABLE_NAME, "a"), Range.closed(1, 3), INTEGER);
        Attribute attributeB = new Attribute(new ColumnIdentifier(TABLE_NAME, "b"), Range.closed(3, 4), INTEGER);
        Attribute attributeC = new Attribute(new ColumnIdentifier(TABLE_NAME, "c"), Range.closed(1, 3), INTEGER);
        Attribute attributeD = new Attribute(new ColumnIdentifier(TABLE_NAME, "d"), Range.closed(1, 4), INTEGER);
        ImmutableList<Attribute> attributes = ImmutableList.of(attributeA, attributeB, attributeC, attributeD);
        TableInfo tableInfo = new TableInfo(TABLE_NAME, attributes);
        InclusionDependency indAC = toInd(attributeA.getColumnIdentifier(), attributeC.getColumnIdentifier());
        InclusionDependency indAD = toInd(attributeA.getColumnIdentifier(), attributeD.getColumnIdentifier());
        InclusionDependency indCA = toInd(attributeC.getColumnIdentifier(), attributeA.getColumnIdentifier());
        InclusionDependency indCD = toInd(attributeC.getColumnIdentifier(), attributeD.getColumnIdentifier());
        InclusionDependency indBD = toInd(attributeB.getColumnIdentifier(), attributeD.getColumnIdentifier());
        ImmutableSet<InclusionDependency> validInds = ImmutableSet.of(indAC, indAD, indCA, indCD, indBD);

        when(dataAccessObject.isValidUIND(any(InclusionDependency.class)))
                .thenAnswer(invocation -> validInds.contains(invocation.<InclusionDependency>getArgument(0)));

        // WHEN
        when(dataAccessObject.getTableInfo(TABLE_NAME)).thenReturn(tableInfo);
        bellBrockhausen.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(validInds));
    }


    @Test
    public void testTableWithEqualValues(DataAccessObject dataAccessObject) throws AlgorithmExecutionException {
        // GIVEN
        Attribute attributeA = new Attribute(new ColumnIdentifier(TABLE_NAME, "a"), Range.closed(1, 3), INTEGER);
        Attribute attributeB = new Attribute(new ColumnIdentifier(TABLE_NAME, "b"), Range.closed(1, 3), INTEGER);
        Attribute attributeC = new Attribute(new ColumnIdentifier(TABLE_NAME, "c"), Range.closed(1, 3), INTEGER);
        ImmutableList<Attribute> attributes = ImmutableList.of(attributeA, attributeB, attributeC);
        TableInfo tableInfo = new TableInfo(TABLE_NAME, attributes);
        InclusionDependency indAB = toInd(attributeA.getColumnIdentifier(), attributeB.getColumnIdentifier());
        InclusionDependency indBA = toInd(attributeB.getColumnIdentifier(), attributeA.getColumnIdentifier());
        InclusionDependency indAC = toInd(attributeA.getColumnIdentifier(), attributeC.getColumnIdentifier());
        InclusionDependency indCA = toInd(attributeC.getColumnIdentifier(), attributeA.getColumnIdentifier());
        InclusionDependency indBC = toInd(attributeB.getColumnIdentifier(), attributeC.getColumnIdentifier());
        InclusionDependency indCB = toInd(attributeC.getColumnIdentifier(), attributeB.getColumnIdentifier());
        ImmutableSet<InclusionDependency> validInds = ImmutableSet.of(indAB, indBA, indAC, indCA, indBC, indCB);

        when(dataAccessObject.isValidUIND(any(InclusionDependency.class)))
                .thenAnswer(invocation -> validInds.contains(invocation.<InclusionDependency>getArgument(0)));

        // WHEN
        when(dataAccessObject.getTableInfo(TABLE_NAME)).thenReturn(tableInfo);
        bellBrockhausen.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(validInds));
    }

    @Test
    public void testTableWithMixedTypes(DataAccessObject dataAccessObject) throws AlgorithmExecutionException {
        // GIVEN
        Attribute attributeA = new Attribute(new ColumnIdentifier(TABLE_NAME, "a"), Range.closed(1, 3), INTEGER);
        Attribute attributeB = new Attribute(new ColumnIdentifier(TABLE_NAME, "b"), Range.closed(1, 4), INTEGER);
        Attribute attributeC = new Attribute(new ColumnIdentifier(TABLE_NAME, "d"), Range.closed("b", "c"), TEXT);
        Attribute attributeD = new Attribute(new ColumnIdentifier(TABLE_NAME, "c"), Range.closed("a", "z"), TEXT);
        ImmutableList<Attribute> attributes = ImmutableList.of(attributeA, attributeB, attributeC, attributeD);
        TableInfo tableInfo = new TableInfo(TABLE_NAME, attributes);
        InclusionDependency indAB = toInd(attributeA.getColumnIdentifier(), attributeB.getColumnIdentifier());
        InclusionDependency indCD = toInd(attributeC.getColumnIdentifier(), attributeD.getColumnIdentifier());
        ImmutableSet<InclusionDependency> validInds = ImmutableSet.of(indAB, indCD);

        when(dataAccessObject.isValidUIND(any(InclusionDependency.class)))
                .thenAnswer(invocation -> validInds.contains(invocation.<InclusionDependency>getArgument(0)));

        // WHEN
        when(dataAccessObject.getTableInfo(TABLE_NAME)).thenReturn(tableInfo);
        bellBrockhausen.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(validInds));
    }

    private InclusionDependency toInd(ColumnIdentifier dependant, ColumnIdentifier referenced) {
        return new InclusionDependency(new ColumnPermutation(dependant), new ColumnPermutation(referenced));
    }

    private InclusionDependency[] toArray(Set<InclusionDependency> inds) {
        return inds.stream().toArray(InclusionDependency[]::new);
    }
}
