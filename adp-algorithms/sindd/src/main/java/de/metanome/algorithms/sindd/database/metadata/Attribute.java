package de.metanome.algorithms.sindd.database.metadata;

import de.metanome.util.TableInfo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Nuhad.Shaabani
 * 
 */
public class Attribute {

	private static int COUNTER = 0;

	private String id;
	private String name;
	private TableInfo table;
	private Set<Attribute> referencedAttributes;

	private Map<TableInfo, Attribute> refAttPerTable;

	public Attribute(String name, TableInfo table) {
		this.name = name;
		this.table = table;
		this.id = String.valueOf(++COUNTER);
		referencedAttributes = new HashSet<Attribute>();

		refAttPerTable = new HashMap<TableInfo, Attribute>();
	}

	public String getId() {
		return id;
	}

	public void initRefAttributes(Collection<Attribute> attributes) {
		referencedAttributes = new HashSet<Attribute>();
		referencedAttributes.addAll(attributes);
	}

	public void updateRefAttributes(Collection<Attribute> attributes) {
		referencedAttributes.retainAll(attributes);
	}

	public boolean isRefAttsInitialized() {
		return referencedAttributes != null && referencedAttributes.size() > 0;
	}

	public Set<Attribute> getRefAttributes() {
		return referencedAttributes;
	}

	public TableInfo getTable() {
		return table;
	}

	public String getTableName() { return table.getTableName(); }

	//Remove
	public String getQTableName() {
		return "";
	}

	public String getQName() {
		return getQTableName() + "." + getName();
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return getQName();
	}
}
