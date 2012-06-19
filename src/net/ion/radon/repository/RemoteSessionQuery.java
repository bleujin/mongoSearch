package net.ion.radon.repository;

import static net.ion.radon.repository.NodeConstants.ID;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ion.framework.db.RepositoryException;
import net.ion.framework.parse.gson.JsonParser;
import net.ion.framework.util.ChainMap;
import net.ion.framework.util.Closure;
import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.StringUtil;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.ics.ActionQuery;
import net.ion.radon.repository.mr.ReduceFormat;
import net.ion.radon.repository.orm.NodeORM;
import net.ion.radon.repository.remote.body.QueryBody;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

public class RemoteSessionQuery extends SessionProtectedQuery{

	private static final long serialVersionUID = -3597031257688988594L;
	private transient SessionQueryImpl squery ;
	private int limit = 0;
	private int skip = 0;

	private RemoteSessionQuery(SessionQueryImpl squery){
		this.squery = squery ;
	}
	
	public static RemoteSessionQuery create(Session session) {
		return new RemoteSessionQuery(SessionQueryImpl.create(session));
	}
	
	public static SessionQuery create(Session session, PropertyQuery definedQuery) {
		return new RemoteSessionQuery(SessionQueryImpl.create(session, definedQuery)); 
	}
	
	public static SessionQuery create(Session session, String wname) {
		return new RemoteSessionQuery(SessionQueryImpl.create(session, wname)) ; 
	}

	public static SessionQuery create(Session session, String wname, WorkspaceOption option) {
		return new RemoteSessionQuery(SessionQueryImpl.create(session, wname, option)); 
	}

	
	public NodeCursor find() throws RepositoryException {
		return RemoteCursor.create(this, Columns.ALL) ;
	}

	public NodeCursor find(Columns columns) throws RepositoryException{
		return RemoteCursor.create(this, columns) ;
	}

	
	
	
	public Node findOne() throws RepositoryException {
		return squery.findOne() ;
	}

	public Node findOne(Columns columns) {
		return squery.findOne(columns) ;
	}
	
	public <T> T findOne(Class<T> clz) {
		return squery.findOne(clz) ;
	}

	
	public boolean existNode(){
		return find().count() > 0;
	}

	public int remove(){
		return squery.remove() ;
	}

	public SessionQuery aradonGroup(String groupId){
		squery.aradonGroup(groupId) ;
		return this ;
	}

	public SessionQuery aradonGroupId(String groupId, Object uId){
		squery.aradonGroupId(groupId, uId) ;
		return this ;
	}

	public SessionQuery path(String path){
		squery.path(path) ;
		return this ;
	}
	

	public SessionQuery eq(String key, Object value) {
		squery.eq(key, value) ;
		return this ;
	}

	public SessionQuery in(String key, Object[] objects) {
		squery.in(key, objects) ;
		return this ;
	}
	
	public SessionQuery nin(String key, Object[] objects){
		squery.nin(key, objects) ;
		return this ;
	}
	public SessionQuery and(PropertyQuery... conds) {
		squery.and(conds) ;
		return this ;
	}

	public SessionQuery or(PropertyQuery... conds) {
		squery.or(conds) ;
		return this ;
	}
	
	public SessionQuery ne(String key, String value) {
		squery.ne(key, value) ;
		return this ;
	}

	public SessionQuery between(String key, Object open, Object close ){
		squery.between(key, open, close) ;
		return this;
	}
	
	public SessionQuery where(String where ){
		squery.where(where) ;
		return this;
	}
	
	public SessionQuery gte(String key, Object value) { // key >= val
		squery.gte(key, value) ;
		return this ;
	}
	public SessionQuery lte(String key, Object value) { // key <= val
		squery.lte(key, value) ;
		return this ;
	}

	public SessionQuery eleMatch(String key, PropertyQuery eleQuery) {
		squery.eleMatch(key, eleQuery) ;
		return this ;
	}

	
	public SessionQuery isExist(String key) {
		squery.isExist(key);
		return this ;
	}
	
	public SessionQuery isNotExist(String key) {
		squery.isNotExist(key) ;
		return this ;
	}


	public SessionQuery gt(String key, Object value) {
		squery.gt(key, value) ;
		return this;
	}

	public SessionQuery lt(String key, Object value) {
		squery.lt(key, value) ;
		return this;
	}
	

	public SessionQuery to(Node target, String relType) {
		squery.to(target, relType) ;
		return this;
	}


	public List<Node> find(PageBean page) throws RepositoryException {
		return find().toList(page);
	}


	public SessionQuery ascending(String... propIds) {
		squery.ascending(propIds) ;
		return this ;
	}
	
	public SessionQuery descending(String... propIds) {
		squery.descending(propIds) ;
		return this ;
	}
	
	public String toString() {
		return squery.toString() ;
	}
	
	public SessionQuery startPathInclude(String path) {
		squery.startPathInclude(path) ;
		return this ;
	}

	public SessionQuery regEx(String key, String regValue) {
		squery.regEx(key, regValue) ;
		return this ;
	}
	
	public SessionQuery id(String oid){
		squery.id(oid) ;
		return this ;
	}

	public SessionQuery idIn(String[] oids){
		squery.idIn(oids) ;
		return this ;
	}
	

	public SessionQuery aquery(String str) {
		squery.aquery(str) ;
		return this ;
	}


	public int count() {
		return squery.count() ;
	}

	// map에 없는 key의 property들은 지워짐
	public boolean overwriteOne(Map<String, ?> map) {
		return squery.overwriteOne(map) ;
	}
	
	// map에 있는 값들만 set, map에 없는 key의 property들은 남아 있음. 
	public boolean updateOne(Map<String, ?> map) {
		return squery.updateOne(map) ;
	}

	public PropertyQuery getQuery(){
		return squery.getQuery() ;
	}

	public NodeResult update(ChainMap modValues){
		return squery.update(modValues) ;
	}

	public NodeResult update(Map<String, ?> modValues){
		return squery.update(modValues);
	}

	public NodeResult merge(ChainMap modValues) {
		return squery.merge(modValues) ;
	}
	
	public NodeResult merge(Map<String, ?> modValues) {
		return squery.merge(modValues) ;
	}

	
	// upset = true, 즉 query에 해당하는 row가 없으면 새로 만든다. 
	public NodeResult increase(String propId){
		return squery.increase(propId) ;
	}
	
	// upset = true, 즉 query에 해당하는 row가 없으면 새로 만든다. 
	public NodeResult increase(String propId, int incvalue){
		return squery.increase(propId, incvalue) ;
	}
	
	public Node findOneInDB() {
		return squery.findOneInDB() ;
	}

	public InListQueryNode inlist(String field) {
		return squery.inlist(field) ;
	}
	
	public NodeCursor format(ReduceFormat format) {
		return squery.format(format) ;
	}
	
	public NodeCursor mapreduce(String mapFunction, String reduceFunction, String finalFunction) {
		return squery.mapreduce(mapFunction, reduceFunction, finalFunction) ;
	}

	public NodeCursor mapreduce(String mapFunction, String reduceFunction, String finalFunction, CommandOption options) {
		return squery.mapreduce(mapFunction, reduceFunction, finalFunction) ;
	}

	public Object apply(String mapFunction, String reduceFunction, String finalFunction, CommandOption options, ApplyHander handler) {
		return squery.apply(mapFunction, reduceFunction, finalFunction, options, handler) ;
	}
	

	
	public NodeCursor group(IPropertyFamily keys, IPropertyFamily initial, String reduce) {
		return squery.group(keys, initial, reduce) ;
	}

	public UpdateChain updateChain() {
		return squery.updateChain() ;
	}

	@Override
	protected PropertyFamily getSort() {
		return squery.getSort();
	}

	@Override
	protected Workspace getWorkspace() {
		return squery.getWorkspace();
	}

	SessionProtectedQuery getSessionQuery(){
		return squery ;
	}
	
	public Session getSession(){
		return squery.getSession() ;
	}
	
	public void setSkip(int n){
		this.skip = n ;
	}

	public void setLimit(int n) {
		this.limit = n ;
	}

	public QueryBody createQueryBody(Columns columns, PageBean page) {
		QueryBody qbody = QueryBody.createDetail(getQuery(), columns, getSort(), this.skip, this.limit, page);
		return qbody ;
	}
	
	
}


class RemoteCursor implements NodeCursor {

	private NodeCursor real ;
	
	private RemoteSessionQuery rquery ;
	private Columns columns;
	private RemoteCursor(RemoteSessionQuery rquery, Columns columns){
		this.rquery = rquery ;
		this.columns = columns ;
	} 
	
	
	public static NodeCursor create(RemoteSessionQuery rquery, Columns columns) {
		return new RemoteCursor(rquery, columns);
	}

	public NodeCursor ascending(String... propIds) {
		rquery.ascending(propIds) ;
		return this;
	}

	public NodeCursor descending(String... propIds) {
		rquery.descending(propIds) ;
		return this;
	}

	public PropertyQuery getQuery() {
		return rquery.getQuery();
	}

	public NodeCursor limit(int n) {
		rquery.setLimit(n) ;
		return this;
	}

	public NodeCursor skip(int n) {
		rquery.setSkip(n) ;
		return this;
	}

	private static Integer ONE = new Integer(1) ;
	public NodeCursor sort(PropertyFamily family) {
		Map map = family.getDBObject().toMap() ;
		
		for (Object key : map.keySet()) {
			Object value = map.get(key) ;
			if (ONE.equals(value)){
				ascending(key.toString()) ;
			} else {
				descending(key.toString()) ;
			}
		}
		
		return this;
	}

	
	
	
	
	
	
	
	
	public int count() {
		return createReal().count();
	}

	public void debugPrint(PageBean page) {
		createReal(page).debugPrint(page) ;
	}

	public <T> void each(PageBean page, Closure<T> closure) {
		createReal(page).each(page, closure) ;
	}

	public Explain explain() {
		return createReal().explain();
	}


	public boolean hasNext() {
		return createReal().hasNext();
	}


	public Node next() {
		return createReal().next();
	}

	public NodeScreen screen(PageBean page) {
		return createReal(page).screen(page);
	}

	public List<Node> toList(PageBean page) {
		return createReal(page).toList(page);
	}

	public List<Node> toList(PageBean page, PropertyComparator comparator) {
		return createReal(page).toList(page, comparator);
	}

	public <T> List<T> toList(PageBean page, Class<? extends NodeORM> clz) {
		return createReal(page).toList(page, clz);
	}

	public List<Map<String, ? extends Object>> toMapList(PageBean page) {
		return createReal(page).toMapList(page);
	}

	public List<Map<String, ? extends Object>> toPropertiesList(PageBean page) {
		return createReal(page).toPropertiesList(page);
	}

	private NodeCursor createReal() {
		return createReal(PageBean.ALL) ;
	}
	
	private synchronized NodeCursor createReal(PageBean page){
		if (real == null){
			// getWorkspace().find(session, inner, Columns.ALL).sort(sort);
			RemoteWorkspace rworkspace = (RemoteWorkspace) rquery.getWorkspace() ;
			real = rworkspace.findDetail(rquery.getSession(), rquery.createQueryBody(this.columns, page)) ;
		} 
		return real ;
	}

	public <T> List<T> toList(Class<T> clz, PageBean page) {
		return createReal(page).toList(clz, page);
	}


	public final static NodeCursor create(Session session, PropertyQuery query, List<Node> nodes, Explain explain){
		session.setAttribute(Explain.class.getCanonicalName(), explain) ;
		return NodeListCursor.create(session, query, nodes) ;
	}
}