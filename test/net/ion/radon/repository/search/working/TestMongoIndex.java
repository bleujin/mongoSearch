package net.ion.radon.repository.search.working;

import junit.framework.TestCase;
import net.ion.framework.db.Page;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.nsearcher.Searcher;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.indexer.storage.mongo.SimpleCentralConfig;
import net.ion.nsearcher.indexer.storage.mongo.StorageFac;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

import org.apache.lucene.store.Directory;

public class TestMongoIndex extends TestCase {

	private Central c;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Directory dir = StorageFac.createToMongo("61.250.201.78", "itest", "myin");
		this.c = SimpleCentralConfig.create(dir).build() ;
	}
	
	@Override
	protected void tearDown() throws Exception {
		c.close() ;
		super.tearDown();
	}

	public void testIndex() throws Exception {
		Indexer iw = c.newIndexer();
		iw.index(new MyKoreanAnalyzer(), new IndexJob<Void>(){

			public Void handle(IndexSession session) throws Exception {
				for (int i : ListUtil.rangeNum(100)) {
					session.insertDocument(createDoc(i));
				}
				return null;
			}
			
		});
	}

	public void testRetry() throws Exception {
		for (int i = 0; i < 2; i++) {
			testIndex() ;
		}
	}
	
	public void testSearch() throws Exception {
		Searcher searcher = c.newSearcher();
		searcher.searchTest("355").debugPrint(Page.ALL) ;
	}

	private MyDocument createDoc(int i) {
		MyDocument doc = MyDocument.testDocument();
		doc.add(MyField.number("docid", i));
		doc.add(MyField.keyword("name", RandomUtil.nextRandomString(10)));
		doc.add(MyField.text("subject", RandomUtil.nextRandomString(100)));

		return doc;
	}

}
