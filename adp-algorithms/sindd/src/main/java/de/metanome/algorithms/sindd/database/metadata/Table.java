package de.metanome.algorithms.sindd.database.metadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @author nuhad.shaabani
 *
 */
public class Table implements Iterable<Attribute> {

	private String name;

	private List<Attribute> attributes;

	public Table(String name) {
		this.name = name;
		attributes = new ArrayList<Attribute>();
	}

	public void addAttribute(Attribute attribute) {
		attributes.add(attribute);
	}

	public String getName() {
		return name;
	}

	@Override
	public Iterator<Attribute> iterator() {
		return attributes.iterator();
	}
}
