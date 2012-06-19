package net.ion.radon.repository.remote.body;

import java.io.Serializable;

import net.ion.radon.repository.IPropertyFamily;

public class WorkspaceBody implements Serializable{

	private static final long serialVersionUID = 5621061970432800758L;
	private boolean unique;
	private String indexName;
	private IPropertyFamily props;

	private WorkspaceBody(IPropertyFamily props, String indexName, boolean unique) {
		this.props = props ;
		this.indexName = indexName ;
		this.unique = unique ;
	}


	public static Object create(IPropertyFamily props, String indexName, boolean unique) {
		return new WorkspaceBody(props, indexName, unique);
	}
	
	public boolean isUnique() {
		return unique;
	}

	public String getIndexName() {
		return indexName;
	}

	public IPropertyFamily getProp() {
		return props;
	}


}
