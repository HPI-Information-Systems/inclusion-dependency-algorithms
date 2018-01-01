package de.metanome.algorithms.zigzag;

import static org.junit.Assert.assertEquals;

import com.google.inject.Inject;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class ZigzagTest {

	@Inject
  private Zigzag zigzag;

	@Test
	public void testCalculateOptimisticBorder() throws AlgorithmExecutionException{
		Set<InclusionDependency> unsatisfiedINDs = new HashSet<>();

		ColumnIdentifier a1 = new ColumnIdentifier("table", "a");
		ColumnIdentifier a2 = new ColumnIdentifier("table", "a2");
    ColumnIdentifier b1 = new ColumnIdentifier("table", "b");
    ColumnIdentifier b2 = new ColumnIdentifier("table", "b2");
		ColumnIdentifier c1 = new ColumnIdentifier("table", "c");
		ColumnIdentifier c2 = new ColumnIdentifier("table", "c2");
		ColumnIdentifier d1 = new ColumnIdentifier("table", "d");
		ColumnIdentifier d2 = new ColumnIdentifier("table", "d2");
		ColumnIdentifier e1 = new ColumnIdentifier("table", "e");
		ColumnIdentifier e2 = new ColumnIdentifier("table", "e2");

		ColumnPermutation ad1 = new ColumnPermutation(a1,d1);
		ColumnPermutation ad2 = new ColumnPermutation(a2,d2);
		InclusionDependency AD = new InclusionDependency(ad1, ad2);

		ColumnPermutation cd1 = new ColumnPermutation(c1,d1);
		ColumnPermutation cd2 = new ColumnPermutation(c2,d2);
		InclusionDependency CD = new InclusionDependency(cd1, cd2);

		ColumnPermutation de1 = new ColumnPermutation(d1,e1);
		ColumnPermutation de2 = new ColumnPermutation(d2,e2);
		InclusionDependency DE = new InclusionDependency(de1, de2);

		unsatisfiedINDs.add(AD);
		unsatisfiedINDs.add(CD);
		unsatisfiedINDs.add(DE);

    ColumnPermutation bd1 = new ColumnPermutation(b1,d1);
    ColumnPermutation bd2 = new ColumnPermutation(b2,d2);
    InclusionDependency BD = new InclusionDependency(bd1, bd2);

    ColumnPermutation abce1 = new ColumnPermutation(a1,b1,c1,e1);
    ColumnPermutation abce2 = new ColumnPermutation(a2,b2,c2,e2);
    InclusionDependency ABCE = new InclusionDependency(abce1, abce2);

    Set<InclusionDependency> optimisticBorder = new HashSet<>();
    optimisticBorder.add(BD);
    optimisticBorder.add(ABCE);

		assertEquals(optimisticBorder, zigzag.calculateOptimisticBorder(unsatisfiedINDs));
	}

	private InclusionDependency makeUnaryIND(ColumnIdentifier dep, ColumnIdentifier ref) {
		return new InclusionDependency(new ColumnPermutation(dep), new ColumnPermutation(ref));
	}
}
