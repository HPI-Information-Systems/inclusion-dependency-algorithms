package de.metanome.algorithms.sindd.database.metadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @author Nuhad.Shaabani
 *
 */
public class Schema implements Iterable<Table> {
	
	private List<Table> tables;
	
	public Schema() {
		tables = new ArrayList<Table>();
	}
	
	public void addTable(Table table) {
		tables.add(table);
	}
	
	@Override
	public Iterator<Table> iterator() {
		return tables.iterator();
	}
	
	public List<Table> getTables() {
		return tables;
	}
	
}
