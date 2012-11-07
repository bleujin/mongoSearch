package net.ion.radon.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import net.ion.framework.util.MapUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.JobEntry;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.collection.CollectionFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class SearchSession implements Session {
	private final static int DEFAULT_RESYNC_INTERVAL = 1000;
	
	private Session inner;
	private Central central ;
	private Analyzer analyzer;
	private List<String> ignoreBodyField; 
	
	private Map<String, SearchWorkspace> wss = MapUtil.newCaseInsensitiveMap() ;
	
	private SearchSession(Session inner, Central central, Analyzer analyzer) {
		this.inner = inner;
		this.central = central ;
		this.analyzer = analyzer;
		this.ignoreBodyField = new ArrayList<String>();
	}

	static SearchSession create(Session inner, Central central, Analyzer analyzer) {
		return new SearchSession(inner, central, analyzer);
	}

	public Session changeWorkspace(String wname) {
		inner.changeWorkspace(wname);
		return this;
	}

	public Session changeWorkspace(String wname, WorkspaceOption options) {
		inner.changeWorkspace(wname, options);
		
		return this ;
	}

	public void clear() {
		inner.clear();
	}

	public int commit() {
		getCurrentWorkspace().updateIndexOnCommit(this, inner.getModified().toArray(new Node[0]));
		int result = inner.commit();
		return result;
	}
	
	private SearchWorkspace findWorkspace(String wsName, WorkspaceOption option){
		if (wss.containsKey(wsName)){
			return wss.get(wsName) ;
		} else {
			SearchWorkspace newWorkspace = SearchWorkspace.load(this, wsName, option) ;
			wss.put(wsName, newWorkspace) ;
			return newWorkspace ;
		}
	}

	public Node createChild(Node parent, String name) {
//		return inner.createChild(parent, name);
		final NodeImpl newNode = NodeImpl.create(this, getCurrentWorkspaceName(), NodeObject.create(), parent.getPath(), name);
		newNode.toRelation(NodeConstants.PARENT, parent.selfRef());

		return newNode;
	}

	public SessionQuery createQuery() {
		return SessionQueryImpl.create(this);
	}

	public SessionQuery createQuery(PropertyQuery definedQuery) {
		return SessionQueryImpl.create(this, definedQuery);
	}

	public SessionQuery createQuery(String wname, WorkspaceOption option) {
		return SessionQueryImpl.create(this, wname, option);
	}

	public SessionQuery createQuery(String wname) {
		return SessionQueryImpl.create(this, wname) ;
	}

	public void dropWorkspace() {
		getCurrentWorkspace().drop() ;
	}

	public <T> T getAttribute(String key, Class<T> T) {
		return inner.getAttribute(key, T);
	}

	public SearchWorkspace getCurrentWorkspace() {
		return findWorkspace(inner.getCurrentWorkspaceName(), WorkspaceOption.EMPTY);
	}

	public String getCurrentWorkspaceName() {
		return inner.getCurrentWorkspaceName();
	}

	public Collection<Node> getModified() {
		return inner.getModified();
	}

	public Node getRoot() {
		return inner.getRoot();
	}

	public ISequence getSequence(String prefix, String id) {
		return inner.getSequence(prefix, id);
	}

	public Workspace getWorkspace(String wname) {
		return SearchWorkspace.load(this, wname);
	}

	public Workspace getWorkspace(String wname, WorkspaceOption option) {
		return SearchWorkspace.load(this, wname, option);
	}

	public String[] getWorkspaceNames() {
		return inner.getWorkspaceNames();
	}

	public void logout() {
		inner.logout();
	}

	public Node newNode() {
		return inner.getCurrentWorkspace().newNode(this);
	}

	public Node newNode(String name) {
		return inner.getCurrentWorkspace().newNode(this, name);
	}

	public TempNode tempNode() {
		return TempNodeImpl.create(this, NodeObject.create());
	}

	public SearchQuery createSearchQuery() {
		return SearchQuery.create(this);
	}

	public int resyncIndex() {
		return resyncIndex(DEFAULT_RESYNC_INTERVAL);
	}

	public int resyncIndex(int interval) {
		deleteQuery(new TermQuery(new Term(NodeConstants.WSNAME, inner.getCurrentWorkspace().getName())));
		waitForFlushed();
		return resyncIndex(PropertyQuery.create(), interval);
	}
	
	public int resyncIndex(PropertyQuery definedQuery) {
		return resyncIndex(definedQuery, DEFAULT_RESYNC_INTERVAL);
	}

	public int resyncIndex(PropertyQuery definedQuery, int interval) {
		int totalCnt = inner.createQuery(definedQuery).count();
		int result = 0;
		System.out.print("#resyncIndex Start..\n");

		for (int i = 1, last = ((totalCnt / interval) + 1); i <= last; i++) {

			final List<Node> nodeList = inner.createQuery(definedQuery).find(PageBean.create(interval, i));
			Future<Integer> fu = addJobEntry(new JobEntry<Integer>() {

				@Override
				public Analyzer getAnalyzer() {
					return analyzer;
				}

				@Override
				public Integer handle(IWriter writer) throws IOException {
					int result = 0;
					for (Node node : nodeList) {
						MyDocument doc = SearchWorkspace.createDocument(node);
						doc.setIgnoreBodyField(ignoreBodyField);
						writer.updateDocument(doc);
						result++;
					}
					return result;
				}
			});
			try {
				result += fu.get();
				System.out.print("#resyncIndexing(" + result + ")\n");

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.print("#resyncIndex End(" + result + ")\n");
		return result;
	}

	private void deleteQuery(final Query query) {
		addJobEntry(new JobEntry<Integer>() {

			@Override
			public Analyzer getAnalyzer() {
				return analyzer;
			}

			@Override
			public Boolean handle(IWriter writer) throws IOException {
				writer.deleteQuery(query);
				return true;
			}
		});

	}
	
	public void addIgnoreBodyField(String... ignoreBodyFields) {
		for(String ignoreField : ignoreBodyFields) {
			if(!ignoreBodyField.contains(ignoreBodyField)){
				ignoreBodyField.add(ignoreField);
			}
		}
	}
	
	public NodeResult merge(String idOrPath, TempNode tnode) {
		return inner.merge(idOrPath, tnode);
	}

	public NodeResult merge(MergeQuery query, TempNode tnode) {
		return getCurrentWorkspace().merge(this, query, tnode);
	}

	public NodeResult remove(Node node) {
		if (node == getRoot())
			return NodeResult.NULL;
		return getWorkspace(node.getWorkspaceName()).remove(this, PropertyQuery.createById(node.getIdentifier()));
	}

	public Node mergeNode(MergeQuery mergeQuery, String... props) {
		return inner.mergeNode(mergeQuery, props);
	}

	public void notify(Node target, NodeEvent event) {
		inner.notify(target, event);
	}

	public void setAttribute(String key, Object value) {
		inner.setAttribute(key, value);
	}

	Central getCentral(){
		return central ;
	}
	
	Session getReal() {
		return inner;
	}

	public void waitForFlushed() {
		central.newDaemonHander().waitForFlushed() ;
	}

	Analyzer getAnalyzer() {
		return analyzer;
	}

	<T> Future<T> addJobEntry(JobEntry<T> indexJob) {
		return central.newDaemonHander().addIndexJob(indexJob) ;
	}

	public <T> T getIndexInfo(IndexInfoHandler<T> indexInfo) {
		return indexInfo.handle(this, getCentral());
	}

	public CollectionFactory newCollectionFactory(String arg0) {
		throw new UnsupportedOperationException("newCollectionFactory") ;
	}
}
