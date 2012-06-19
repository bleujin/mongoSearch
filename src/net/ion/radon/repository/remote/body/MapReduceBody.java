package net.ion.radon.repository.remote.body;

import java.io.Serializable;

import org.apache.ecs.xhtml.code;

import net.ion.radon.repository.CommandOption;
import net.ion.radon.repository.PropertyQuery;

public class MapReduceBody implements Serializable{

	private static final long serialVersionUID = 3020191537870266060L;
	private PropertyQuery query ;
	private CommandOption options ;
	private String mapFn = "";
	private String reduceFn = "";
	private String finalFn = "";

	private MapReduceBody(PropertyQuery query, CommandOption options, String mapFn, String reduceFn, String finalFn) {
		this.query = query ;
		this.options = options ;
		this.mapFn = mapFn ;
		this.reduceFn = reduceFn ;
		this.finalFn = finalFn ;
	}

	public final static MapReduceBody create(PropertyQuery query, CommandOption options, String mapFn, String reduceFn, String finalFn){
		return new MapReduceBody(query, options, mapFn, reduceFn, finalFn) ;
	}
	
	public String mapFn() {
		return mapFn;
	}

	public String reduceFn() {
		return reduceFn;
	}

	public String finalFn() {
		return finalFn;
	}
	
	public CommandOption options(){
		return options ;
	}

	public PropertyQuery getQuery() {
		return query;
	}

}
