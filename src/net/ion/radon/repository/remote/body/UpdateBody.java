package net.ion.radon.repository.remote.body;

import java.io.Serializable;

import net.ion.radon.repository.PropertyQuery;

import com.mongodb.DBObject;

public class UpdateBody implements Serializable{

	private static final long serialVersionUID = -8560792753371012413L;
	private PropertyQuery query ;
	private DBObject values ;
	private Boolean upset ;
	private Boolean multi ;
	
	private UpdateBody(PropertyQuery query, DBObject values, boolean upset, boolean multi) {
		this.query = query ;
		this.values = values ;
		this.upset = upset ;
		this.multi = multi ;
	}

	public static UpdateBody create(PropertyQuery query, DBObject values, boolean upset, boolean multi) {
		return new UpdateBody(query, values, upset, multi) ;
	}

	public PropertyQuery getQuery() {
		return query;
	}

	public boolean multi() {
		return multi;
	}
	
	public boolean upset(){
		return upset ;
	}

	public DBObject getValues() {
		return values;
	} 
	

}
