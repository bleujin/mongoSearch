package net.ion.nsearcher.directory;

import java.io.IOException;
import java.util.concurrent.Future;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.nsearcher.ISTestCase;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.config.CentralConfig;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.SearchRequest;
import net.ion.nsearcher.search.Searcher;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;
import net.ion.nsearcher.search.processor.StdOutProcessor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class TestDirectory extends ISTestCase{

	public void testCopy() throws Exception {
		final Central cs = CentralConfig.newRam().build() ;
		Central ct = CentralConfig.newRam().build() ;
		
		Analyzer anal = new MyKoreanAnalyzer() ;

		Future<Boolean> f1 = cs.newIndexer().asyncIndex("hero", anal, new MyIndexJob("hero"));
		f1.get() ;
		
		Future<Boolean> f2 = ct.newIndexer().asyncIndex("bleujin", anal, new MyIndexJob("bleujin"));
		f2.get() ;
		
		ct.newIndexer().index(new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				session.appendFrom(cs.dir()) ;
				return null;
			}
		}) ;
		
		assertEquals(10, ct.newSearcher().search("").size()) ;
	}
	
	
	public void testMerge() throws Exception {
		final Central cs = CentralConfig.newRam().build() ;
		Central ct = CentralConfig.newRam().build() ;

		MyDocument[] docs = new MyDocument[5] ;
		for (int i = 0; i < docs.length; i++) {
			docs[i] = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.keyword("name", "jin")) ;
		}
		
		Analyzer analyzer = new MyKoreanAnalyzer();
		addDocTo(cs, docs, analyzer);

		ct.newIndexer().index(analyzer, new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				session.appendFrom(cs.dir()) ;
				return null;
			}
		}) ;
		
		cs.newSearcher().search("").debugPrint() ;
		Debug.line() ;

		for (int i = 0; i < docs.length; i++) {
			docs[i].add(MyField.keyword("newname", "hero")) ;
		}
		
		addDocTo(cs, docs, analyzer);
		cs.newSearcher().search("").debugPrint() ;
		Debug.line() ;

		
		ct.newIndexer().index(analyzer, new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				session.appendFrom(cs.dir()) ;
				return null;
			}
		}) ;
		ct.newSearcher().search("").debugPrint() ;
	}
	

	public void testDirCopy() throws Exception {
		Central cen1 = writeDocument() ;
		final Central cen2 = write2Dir() ;

		Searcher searcher = cen1.newSearcher() ;
		searcher.addPostListener(new StdOutProcessor()) ;
		assertEquals(6, searcher.createRequest("bleujin").find().size()) ;

		cen1.newIndexer().index(new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				session.appendFrom(cen2.dir()) ;
				return null;
			}
		}) ;
		
		searcher = cen1.newSearcher() ;
		searcher.search("").debugPrint() ;
		
		assertEquals(9, searcher.createRequest("bleujin").find().size()) ;
		
		searcher = cen1.newSearcher() ;
		searcher.addPostListener(new StdOutProcessor()) ;
		assertEquals(9, searcher.createRequest("bleujin").find().size()) ;
		
		
		searcher = cen2.newSearcher() ;
		searcher.addPostListener(new StdOutProcessor()) ;
		assertEquals(3, searcher.createRequest("bleujin").find().size()) ;
	}


	private void addDocTo(Central cs, final MyDocument[] docs, Analyzer analyzer) throws LockObtainFailedException, IOException {
		Indexer iwcs = cs.newIndexer() ;
		iwcs.index(analyzer, new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < docs.length; i++) {
					session.updateDocument(docs[i]) ;
				}
				return null;
			}
		}) ;
	}
	
	
	public void testLockId() throws Exception {
		Directory d1 = new RAMDirectory() ;
		Directory d2 = new RAMDirectory() ;
		
		Debug.line(d1.getLockID(), d2.getLockID()) ;
	}
	
	private Central write2Dir() throws LockObtainFailedException, IOException {
		Central cen = CentralConfig.newRam().build() ;
		Indexer writer = cen.newIndexer() ;
		writer.index(new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 3 ; i++) {
					MyDocument doc = MyDocument.testDocument() ;
					doc.add(MyField.text("name", "bleujin")) ;
					session.insertDocument(doc) ;
				}
				return null;
			}
		}) ;

		return cen ;
	}

	
	public void testAppendFrom() throws Exception {
		final Central tempCen = CentralConfig.newRam().build() ;
		
		Indexer tempWriter = tempCen.newIndexer() ;
		tempWriter.index(new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				MyDocument doc = MyDocument.testDocument().add(MyField.text("name", "bleujin"));
				session.insertDocument(doc) ;
				return null;
			}
		}) ;
		
		Central cen = CentralConfig.newRam().build() ;
		Indexer writer = cen.newIndexer() ;
		writer.index(new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				MyDocument doc = MyDocument.testDocument().add(MyField.text("name", "hero"));
				session.insertDocument(doc) ;
				session.appendFrom(tempCen.dir()) ;
				return null;
			}
		}) ;
		
		Searcher searcher = cen.newSearcher() ;
		assertEquals(2, searcher.createRequest("").find().size()) ;
	}
	
}

class MyIndexJob implements IndexJob<Boolean> {
	String name ;
	MyIndexJob(String name){
		this.name = name ;
	}
	
	public Boolean handle(IndexSession session) throws IOException {
		for (int i : ListUtil.rangeNum(5)) {
			session.insertDocument(MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.keyword("name", name))) ;
		}
		return true;
	}
}
