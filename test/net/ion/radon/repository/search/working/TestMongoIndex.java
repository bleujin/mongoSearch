package net.ion.radon.repository.search.working;

import java.util.Map;

import org.apache.lucene.store.Directory;

import com.mongodb.Mongo;

import net.ion.framework.db.Page;
import net.ion.framework.util.ChainMap;
import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.MapUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.exception.ISearcerException;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.ISearcher;
import net.ion.isearcher.indexer.storage.mongo.StorageFac;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.radon.core.PageBean;
import junit.framework.TestCase;

public class TestMongoIndex extends TestCase {

	private Directory dir ;
	private Central c;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.dir = StorageFac.createToMongo("61.250.201.78", "itest", "myin");
		this.c = Central.createOrGet(dir);
	}
	
	@Override
	protected void tearDown() throws Exception {
		c.clearStore() ;
		dir.close() ;
		super.tearDown();
	}

	public void testIndex() throws Exception {
		IWriter iw = c.newIndexer(new MyKoreanAnalyzer());
		try {
			iw.begin("mytest..");

			for (int i : ListUtil.rangeNum(100)) {
				iw.insertDocument(createDoc(i));
			}
//			iw.optimize();
			iw.end() ;
		} finally {
			// iw.end();
			iw.close();
		}
	}

	public void testRetry() throws Exception {
		for (int i = 0; i < 2; i++) {
			testIndex() ;
		}
	}
	
	public void testSearch() throws Exception {
		ISearcher searcher = c.newSearcher();
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
