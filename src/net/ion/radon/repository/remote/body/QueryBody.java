package net.ion.radon.repository.remote.body;

import java.io.Serializable;

import net.ion.radon.core.PageBean;
import net.ion.radon.repository.Columns;
import net.ion.radon.repository.PropertyFamily;
import net.ion.radon.repository.PropertyQuery;
import net.ion.radon.repository.RemoteSessionQuery;

public class QueryBody implements Serializable{

	private static final long serialVersionUID = -1586544018997969183L;
	private final PropertyQuery iquery ;
	private final Columns columns ;
	private final PropertyFamily sort ;
	private final int skip ;
	private final int limit ;
	private final PageBean page ; // default limit ;
	
	private QueryBody(PropertyQuery iquery, Columns columns, PropertyFamily sort, int skip, int limit, PageBean page) {
		this.iquery = iquery ;
		this.columns = columns ;
		this.sort = sort ;
		this.skip = skip ;
		this.limit = limit ;
		this.page = page ;
	}

	public static QueryBody create(PropertyQuery iquery, Columns columns) {
		return createDetail(iquery, columns, PropertyFamily.BLANK, 0, 0, PageBean.create(100, 1)) ;
	}

	public static QueryBody createDetail(PropertyQuery iquery, Columns columns, PropertyFamily sort, int skip, int limit, PageBean page) {
		QueryBody querybody = new QueryBody(iquery, columns, sort, skip, limit, page);
		return querybody ;
	}

	public PropertyQuery getQuery() {
		return iquery;
	}

	public Columns getColumns() {
		return columns;
	}

	public PageBean getPage() {
		return page;
	}
	
	public int getLimit(){
		return limit ;
	}

	public int getSkip(){
		return skip ;
	}
	
	public PropertyFamily getSort(){
		return sort ;
	}
	
	
}
