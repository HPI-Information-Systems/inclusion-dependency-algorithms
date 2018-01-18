package de.metanome.algorithms.binder;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.BitSet;
/**
 * Created by maxi on 23.12.17.
 */
public class BucketMetadata {
    private int[] bucketComparisonOrder;
    private BitSet nullValueColumns;
    private LongArrayList columnSizes;


    public BucketMetadata(int[] bucketComparisonOrder, BitSet nullValueColumns, LongArrayList columnSizes) {
        this.bucketComparisonOrder = bucketComparisonOrder;
        this.nullValueColumns = nullValueColumns;
        this.columnSizes = columnSizes;
    }

    public LongArrayList getColumnSizes() {
        return columnSizes;
    }

    public void setColumnSizes(LongArrayList columnSizes) {
        this.columnSizes = columnSizes;
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
