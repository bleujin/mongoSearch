package net.ion.nsearcher.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Future;

import junit.framework.TestCase;
import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.nsearcher.Searcher;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.config.CentralConfig;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.indexer.storage.mongo.SimpleCentralConfig;
import net.ion.nsearcher.indexer.storage.mongo.StorageFac;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class TestDaemonWriter extends TestCase {

	public void testNotConflict() throws Exception {
		Central c = CentralConfig.newRam().build() ;

		Indexer wr1 = c.newIndexer();
		Indexer wr2 = c.newIndexer();

		Future<Void> f1 = wr1.asyncIndex("wr1", new MyKoreanAnalyzer(), new IndexJob<Void>() {

			public Void handle(IndexSession session) throws Exception {
				String[] names = new String[] { "bleujin", "jin", "hero" };
				for (int i : ListUtil.rangeNum(10)) {
					MyDocument doc = MyDocument.testDocument();
					doc.add(MyField.number("age", RandomUtil.nextInt(60) + 10)).add(MyField.keyword("name", names[RandomUtil.nextInt(3)]));
					session.insertDocument(doc);
				}
				return null;
			}
		});
		
		Future<Void> f2 = wr2.asyncIndex("wr2", new MyKoreanAnalyzer(), new IndexJob<Void>() {

			public Void handle(IndexSession session) throws Exception {
				MyDocument otherDoc = MyDocument.testDocument().add(MyField.number("age", 20)).add(MyField.keyword("name", "jini"));
				session.insertDocument(otherDoc);
				return null;
			}
		});
		
		f2.get() ;
		
		Searcher searcher = c.newSearcher();
		assertEquals(1, searcher.searchTest("jini").getTotalCount());
		
		f1.get() ;

		searcher = c.newSearcher();
		assertEquals(11, searcher.searchTest("").getTotalCount());
	}

	
	public void testWaitingSession() throws Exception {
		Central c = CentralConfig.newRam().build() ;
		
		IndexJob<Boolean> job = new IndexJob<Boolean>() {
			public Boolean handle(IndexSession session) throws IOException {
				session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "bleujin"))) ;
				return true;
			}
		} ;
		
		Future<Boolean> future = c.newIndexer().asyncIndex("", new MyKoreanAnalyzer(), job);
		future.get() ;
		
		Searcher searcher = c.newSearcher();
		assertEquals(1, searcher.searchTest("").getTotalCount());
	}
	
	
	public void testSpeed() throws Exception {
		// Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "test", "stest") ;
		Central c = CentralConfig.newRam().build() ;

		final String readString = IOUtil.toString(new FileInputStream("libsource/build_fat.xml"));

		Indexer wr = c.newIndexer() ;
		long start = System.currentTimeMillis() ;
		wr.index(new MyKoreanAnalyzer(), new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				for (int i : ListUtil.rangeNum(1000)) {
					MyDocument doc = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.text("file", readString)) ;
					session.insertDocument(doc) ;
				}
				return null;
			}
		}) ;
		Debug.line(wr, System.currentTimeMillis()- start) ;

		// expect ( 8hour) -> 330 per sec
		// if keyword : 370 per sec
		// if text(3k use MyKoreAnalyzer) : 60 per sec... -_-
		// if text(3k user StandardAnalyzer) : 100 per sec.. -_-)
		// if 3k text & ramdir & mykorean : 200 per sec ... -_-
	}
	
	public void xtestOverCount() throws Exception {
		Directory dir = new RAMDirectory() ;
		Central c = CentralConfig.newRam().build() ;

		Indexer wr = c.newIndexer() ;
		wr.index(new MyKoreanAnalyzer(), new IndexJob<Void>() {

			public Void handle(IndexSession session) throws Exception {
				for (int i : ListUtil.rangeNum(100)) {
					MyDocument doc = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.text("name", "bleujin")) ;
					session.insertDocument(doc) ;
				}
				return null;
			}
		}) ;

		Searcher searcher = c.newSearcher() ;
		searcher.searchTest("").debugPrint(Page.ALL) ;
		assertEquals(100, searcher.searchTest("").getTotalCount()) ;
	}
	
	
	public void testLength() throws Exception {
		// Directory dir = new RAMDirectory();
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "test", "stest") ;
		Central c = SimpleCentralConfig.createCentral(dir);

		Indexer wr = c.newIndexer() ;
		final String readString = IOUtil.toString(new FileInputStream("libsource/build_fat.xml")); //3745 byte
		MyKoreanAnalyzer analyzer = new MyKoreanAnalyzer();
		
		wr.index(analyzer, new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				for (int i : ListUtil.rangeNum(1000)) {
					MyDocument doc = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.text("file", readString)) ;
					session.insertDocument(doc) ;
				}
				return null;
			}
		}) ;
		
		long sum = 0 ;
		for(String fileName : dir.listAll()){
			sum += dir.fileLength(fileName) ;
		}
		Debug.line(sum) ; // expect 3,745,000 actual 5,242,565 
	}

}
