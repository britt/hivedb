package org.hivedb.meta.persistence;

import org.hivedb.util.HiveUtils;

public class TableInfo {
	private String name;
	private String createStatement;
	
	public TableInfo(String name, String stmt){
		setName(name);
		setCreateStatement(stmt);
	}

	public String getCreateStatement() {
		return createStatement;
	}

	public String getDeleteAllStatement() {
		return String.format("delete from %s", name);
	}
	
	public void setCreateStatement(String createStatement) {
		this.createStatement = createStatement;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public int hashCode() {
		return HiveUtils.makeHashCode(
			new Object[] {getName(), getCreateStatement()}
		);
	} 
}
