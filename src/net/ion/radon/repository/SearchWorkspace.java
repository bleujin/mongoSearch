package net.ion.radon.repository;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.ion.framework.util.Closure;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.StringUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.common.SearchConstant;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.search.filter.TermFilter;
import net.ion.radon.core.PageBean;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class SearchWorkspace extends Workspace {

	private SearchSession searchSession;
	private String wname;
	private WorkspaceOption option;

	private SearchWorkspace(SearchSession searchSession, String wname, WorkspaceOption option) {
		this.searchSession = searchSession;
		this.wname = wname;
		this.option = option;
	}

	static SearchWorkspace load(SearchSession searchSession, String wname) {
		return new SearchWorkspace(searchSession, wname, WorkspaceOption.EMPTY);
	}

	static SearchWorkspace load(SearchSession searchSession, String wname, WorkspaceOption option) {
		return new SearchWorkspace(searchSession, wname, option);
	}

	public NodeCursor find(Session session, PropertyQuery iquery, Columns columns) {
		return getRealWorkspace().find(session, iquery, columns);
	}

	public String getName() {
		return getRealWorkspace().getName();
	}

	public void drop() {
		getRealWorkspace().drop();
		deleteQuery(searchSession, new TermQuery(new Term(NodeConstants.WSNAME, getRealWorkspace().getName())));
	}

	private Workspace getRealWorkspace() {
		return searchSession.getReal().getWorkspace(wname, option);
	}

	static MyDocument createDocument(Node node) {
		Map<String, ? extends Object> props = node.toPropertyMap();
		MyDocument newDocument = MyDocument.newDocument(node.getIdentifier(), props);
		newDocument.add(MyField.unknown(NodeConstants.ARADON_UID, node.getAradonId().getUid()));

		newDocument.add(MyField.unknown(NodeConstants.ARADON_GROUP, StringUtil.defaultIfEmpty(node.getAradonId().getGroup(), AradonId.EMPTY.getGroup())));
		newDocument.add(MyField.keyword(NodeConstants.WSNAME, node.getWorkspaceName()));
		return newDocument;
	}

	/* update end */
	protected DBCollection getCollection() {
		return getRealWorkspace().getCollection();
	}

	@Override
	public NodeResult remove(Session _session, final PropertyQuery query) {

		final SearchSession session = (SearchSession) _session;

		if (query.getDBObject().keySet().size() == 0) {
			IndexJob<Boolean> job = new IndexJob<Boolean>() {
				public Boolean handle(IndexSession writer) throws IOException {
					writer.deleteQuery(new TermQuery(new Term(NodeConstants.WSNAME, getRealWorkspace().getName())));
					return true;
				}
			};
			session.addJobEntry(job) ;
			
		} else {

			NodeCursor nc = getRealWorkspace().find(session, query, Columns.Meta);
			int i = 1;
			List<String> ids = ListUtil.newList();
			final List<Query> queries = ListUtil.newList() ;
			while (nc.hasNext()) {
				ids.add(nc.next().getIdentifier());
				if (i++ % 100 == 0) {
					queries.add(toQuery(ids));
					ids.clear();
				}
			}
			if (!ids.isEmpty()) {
				queries.add(toQuery(ids));
				ids.clear();
			}
			
			IndexJob<Boolean> job = new IndexJob<Boolean>() {
				public Boolean handle(IndexSession writer) throws IOException {
					for (Query query : queries) {
						writer.deleteQuery(query) ;
					}
					return true ;
				}
			};
			session.addJobEntry(job) ;
			
		}
		NodeResult result = getRealWorkspace().remove(session, query);
		return result;
	}

	private Query toQuery(List<String> ids) {
		if (ids.isEmpty()) {
			return new TermQuery(new Term(SearchConstant.ISKey, "not_exist")) ;
		}
		TermFilter filter = new TermFilter();
		for (String docId : ids) {
			filter.addTerm(new Term(SearchConstant.ISKey, docId));
		}
		return new FilteredQuery(new MatchAllDocsQuery(), filter);
	}

	@Override
	public NodeResult updateNode(Session _session, PropertyQuery query, DBObject values, boolean upset, boolean multi) {

		final List<String> ids = ListUtil.newList();
		SearchSession session = (SearchSession) _session;
		getRealWorkspace().find(session, query, Columns.Meta).each(PageBean.ALL, new Closure<Node>() {
			public void execute(Node node) {
				ids.add(node.getIdentifier());
			}
		});

		NodeResult result = getRealWorkspace().updateNode(session, query, values, upset, true);

		if (upset && ids.size() == 0) {
			getRealWorkspace().find(session, query, Columns.Meta).each(PageBean.ALL, new Closure<Node>() {
				public void execute(Node node) {
					ids.add(node.getIdentifier());
				}
			});
		}

		Node[] modifiedNode = session.createQuery().idIn(ids.toArray(new String[0])).find().toList(PageBean.ALL).toArray(new Node[0]);
		updateIndexOnCommit(session, modifiedNode);

		return result;
	}

	synchronized void updateIndexOnCommit(final SearchSession session, final Node[] targets) {

		IndexJob<Boolean> commitJob = new IndexJob<Boolean>() {
			public Boolean handle(IndexSession writer) throws IOException {
				for (Node node : targets) {
					writer.updateDocument(createDocument(node));
				}
				return true ;
			}
		};

		session.addJobEntry(commitJob);

	}

	@Override
	protected NodeResult updateInner(Session session, PropertyQuery query, DBObject values, boolean upset) {
		return super.updateInner(session, query, values, upset);
	}

	@Override
	protected NodeResult findAndOverwrite(Session session, PropertyQuery query, Map<String, ?> props) {
		Node found = super.findOne(session, query, Columns.Meta);
		if (found == null)
			return NodeResult.NULL;

		NodeResult result = super.findAndOverwrite(session, query, props);

		Node modFoundNode = session.createQuery().id(found.getIdentifier()).findOne();
		updateIndexOnCommit(searchSession, new Node[] { modFoundNode });

		return result;
	}
	private void deleteQuery(final SearchSession session, final Query query) {
		IndexJob<Boolean> indexJob = new IndexJob<Boolean>() {
			public Boolean handle(IndexSession writer) throws IOException {
				writer.deleteQuery(query);
				return true ;
			}
		};

		session.addJobEntry(indexJob);

	}

	public String toString() {
		return getRealWorkspace().toString();
	}

}
