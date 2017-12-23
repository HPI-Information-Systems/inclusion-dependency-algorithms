package de.metanome.algorithms.binder;

import java.util.BitSet;
/**
 * Created by maxi on 23.12.17.
 */
public class BucketizedAttribute {
    private int[] bucketComparisonOrder;
    private BitSet nullValueColumns;

    public BucketizedAttribute(int[] bucketComparisonOrder, BitSet nullValueColumns) {
        this.bucketComparisonOrder = bucketComparisonOrder;
        this.nullValueColumns = nullValueColumns;
    }

    public int[] getBucketComparisonOrder() {
        return bucketComparisonOrder;
    }

    public void setBucketComparisonOrder(int[] bucketComparisonOrder) {
        this.bucketComparisonOrder = bucketComparisonOrder;
    }

    public BitSet getNullValueColumns() {
        return nullValueColumns;
    }

    public void setNullValueColumns(BitSet nullValueColumns) {
        this.nullValueColumns = nullValueColumns;
    }
}
