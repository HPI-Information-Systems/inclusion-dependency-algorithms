package de.metanome.algorithms.sindd.util;

public class PartitionPerformance {

	private int id;
	private long mergingTime;
	private long uindsGenTime;

	public PartitionPerformance(int id) {
		this.id = id;
	}
	
	public int getPartitionId() {
		return id;
	}

	public void setMerginTime(long st, long et) {
		mergingTime = et - st;
	}

	public void setUindsGenTime(long st, long et) {
		uindsGenTime = et - st;
	}

	public long getMergingTime() {
		return mergingTime;
	}

	public long getUindsGenTime() {
		return uindsGenTime;
	}

	public long getTotalTime() {
		return getMergingTime() + getUindsGenTime();
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("merging=[");
		sb.append(TimeUtil.toString(getMergingTime()));
		sb.append("], ");
		sb.append("uindsGen=[");
		sb.append(TimeUtil.toString(uindsGenTime));
		sb.append("], ");
		sb.append(" total=[");
		sb.append(TimeUtil.toString(getTotalTime()));
		sb.append("]");
		return sb.toString();
	}
}
