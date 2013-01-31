package net.ion.radon.repository;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ion.framework.db.Page;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.MapUtil;
import net.ion.framework.util.ObjectUtil;
import net.ion.framework.util.StringUtil;
import net.ion.nsearcher.Searcher;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.search.SearchRequest;
import net.ion.nsearcher.search.SearchResponse;
import net.ion.nsearcher.search.filter.FilterUtil;
import net.ion.nsearcher.search.filter.MatchAllDocsFilter;
import net.ion.nsearcher.search.filter.TermFilter;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.TermRangeFilter;

public class SearchQuery implements Serializable {

	private static final long serialVersionUID = 4799675160767871442L;
	private transient SearchSession session;
	private List<Filter> filters = ListUtil.newList();
	private List<String> sorts = ListUtil.newList();
	private Page page = Page.ALL;
	private Map<String, Object> paramMap = MapUtil.newMap();
	private Filter wsnameFilter ;
	
	private SearchQuery(SearchSession session) {
		this.session = session;
		wsnameFilter = new TermFilter(NodeConstants.WSNAME, session.getCurrentWorkspaceName()) ;
	}

	public static SearchQuery create(SearchSession session) {
		return new SearchQuery(session);
	}

	public SearchQuery term(String field, String value) {
		filters.add(new TermFilter(field, value));
		return this;
	}

	public SearchQuery wsname(String wname) {
		this.wsnameFilter = new TermFilter(NodeConstants.WSNAME, wname) ;
		return this;
	}

	public SearchQuery aradonGroup(String groupName) {
		return term(NodeConstants.ARADON_GROUP, groupName);
	}

	public SearchQuery aradonUid(Object uId) {
		return term(NodeConstants.ARADON_UID, ObjectUtil.toString(uId));
	}

	public SearchQuery aradonId(String groupName, Object uId) {
		return aradonGroup(groupName).aradonUid(uId);
	}

	public SearchQuery between(String field, int min, int max) {
		return between(field, 1L * min, 1L * max);
	}

	public SearchQuery between(String field, long min, long max) {
		filters.add(NumericRangeFilter.newLongRange(field, min, max, true, true));
		return this;
	}

	public SearchQuery between(String field, double min, double max) {
		filters.add(NumericRangeFilter.newDoubleRange(field, min, max, true, true));
		return this;
	}

	public SearchQuery between(String field, String minTerm, String maxTerm) {
		filters.add(FilterUtil.and(TermRangeFilter.Less(field, maxTerm), TermRangeFilter.More(field, minTerm)));
		return this;
	}

	public SearchQuery lt(String field, int max) {
		return lt(field, 1L * max);
	}

	public SearchQuery lt(String field, long max) {
		filters.add(NumericRangeFilter.newLongRange(field, Long.MIN_VALUE, max, true, false));
		return this;
	}

	public SearchQuery lt(String field, double max) {
		filters.add(NumericRangeFilter.newDoubleRange(field, Double.MIN_VALUE, max, true, false));
		return this;
	}

	public SearchQuery lte(String field, String max) {
		filters.add(TermRangeFilter.Less(field, max));
		return this;
	}

	public SearchQuery lte(String field, int max) {
		return lte(field, 1L * max);
	}

	public SearchQuery lte(String field, long max) {
		filters.add(NumericRangeFilter.newLongRange(field, Long.MIN_VALUE, max, true, true));
		return this;
	}

	public SearchQuery lte(String field, double max) {
		filters.add(NumericRangeFilter.newDoubleRange(field, Double.MIN_VALUE, max, true, true));
		return this;
	}

	public SearchQuery gt(String field, int min) {
		return gt(field, 1L * min);
	}

	public SearchQuery gt(String field, long min) {
		filters.add(NumericRangeFilter.newLongRange(field, min, Long.MAX_VALUE, false, true));
		return this;
	}

	public SearchQuery gt(String field, double min) {
		filters.add(NumericRangeFilter.newDoubleRange(field, min, Double.MAX_VALUE, false, true));
		return this;
	}

	public SearchQuery gte(String field, String min) {
		filters.add(TermRangeFilter.More(field, min));
		return this;
	}

	public SearchQuery gte(String field, int min) {
		return gte(field, 1L * min);
	}

	public SearchQuery gte(String field, long min) {
		filters.add(NumericRangeFilter.newLongRange(field, min, Long.MAX_VALUE, true, true));
		return this;
	}

	public SearchQuery gte(String field, double min) {
		filters.add(NumericRangeFilter.newDoubleRange(field, min, Double.MAX_VALUE, true, true));
		return this;
	}

	public SearchQuery addFilter(Filter filter) {
		filters.add(filter);
		return this;
	}

	public SearchQuery ascending(String field) {
		sorts.add(field + " asc");
		return this;
	}

	public SearchQuery descending(String field) {
		sorts.add(field + " desc");
		return this;
	}

	public SearchQuery setPage(Page page) {
		this.page = page;
		return this;
	}

	public SearchQuery addParam(String key, Object value) {
		paramMap.put(key, value);
		return this;
	}

	public SearchResponse find(String query) throws IOException, ParseException {
		Searcher searcher = session.getCentral().newSearcher();

		SearchRequest request = SearchRequest.create(query, null, session.getAnalyzer());
		request.page(page) ;
		
		for (String sort : sorts) {
			if (sort.endsWith(" asc")) request.ascending(StringUtil.substringBefore(sort, " ")) ;
			else request.descending(StringUtil.substringBefore(sort, " ")) ;
		}
		
		for (Entry<String, Object> entry : paramMap.entrySet()) {
			request.setParam(entry.getKey(), entry.getValue());
		}

		request.setFilter(getFilters());

		return searcher.search(request);
	}
    
	public SearchQuery inAllWorkspace() {
		this.wsnameFilter = MatchAllDocsFilter.SELF ;
		return this;
	}
	
	public SearchResponse find() throws IOException, ParseException {
		return find("");
	}

	public MyDocument findOne() throws IOException, ParseException {
		return findOne("");
	}

	public MyDocument findOne(String query) throws IOException, ParseException {
		setPage(Page.create(1, 1)) ;
		List<MyDocument> docs = find(query).getDocument() ;
		return (docs.size() > 0) ? docs.get(0) : null;
	}

	private Filter getFilters() {
		List<Filter> result = new ArrayList<Filter>(filters) ;
		result.add(this.wsnameFilter) ;
		return FilterUtil.and(result);
	}

	

}

