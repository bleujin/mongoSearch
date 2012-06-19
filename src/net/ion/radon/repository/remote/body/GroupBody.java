package net.ion.radon.repository.remote.body;

import java.io.Serializable;

import net.ion.radon.repository.IPropertyFamily;
import net.ion.radon.repository.PropertyQuery;

public class GroupBody implements Serializable{

	private IPropertyFamily keys ;
	private PropertyQuery condition ;
	private IPropertyFamily initial ;
	private String reduceFn ;
	
	private GroupBody(IPropertyFamily keys, PropertyQuery condition, IPropertyFamily initial, String reduceFn) {
		this.keys = keys ;
		this.condition = condition ;
		this.initial = initial ;
		this.reduceFn = reduceFn ;
	}

	public static GroupBody create(IPropertyFamily keys, PropertyQuery condition, IPropertyFamily initial, String reduceFn) {
		return new GroupBody(keys, condition, initial, reduceFn);
	}

	
	public IPropertyFamily keys(){
		return keys ;
	}
	
	public PropertyQuery condition(){
		return condition ;
	}
	
	public IPropertyFamily initial(){
		return initial ;
	}
	
	public String reduceFn(){
		return reduceFn ;
	}
	
}
