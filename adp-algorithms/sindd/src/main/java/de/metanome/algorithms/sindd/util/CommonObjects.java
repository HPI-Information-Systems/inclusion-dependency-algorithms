package de.metanome.algorithms.sindd.util;

import de.metanome.algorithms.sindd.database.metadata.Attribute;
import de.metanome.algorithms.sindd.sindd.Partition;

import java.util.List;
import java.util.Map;

public class CommonObjects {
	private static List<Attribute> attributes;
	private static Map<String, Attribute> id2attributeMap;
	private static List<Partition> partitions;
	private static Performance performance;


	public static void setAttributes(List<Attribute> attributes) {
		CommonObjects.attributes = attributes;
	}

	public static List<Attribute> getAttributes() {
		return attributes;
	}

	public static void setId2attributeMap(Map<String, Attribute> id2attributeMap) {
		CommonObjects.id2attributeMap = id2attributeMap;
	}

	public static Map<String, Attribute> getId2attributeMap() {
		return id2attributeMap;
	}

	public static void setPartitions(List<Partition> partitions) {
		CommonObjects.partitions = partitions;
	}

	public static List<Partition> getPartitions() {
		return partitions;
	}

	public static void setPerformance(Performance performance) {
		CommonObjects.performance = performance;
	}

	public static Performance getPerformance() {
		return performance;
	}
}
