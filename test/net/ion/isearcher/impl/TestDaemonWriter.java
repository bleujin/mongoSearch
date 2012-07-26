package net.ion.isearcher.impl;

import java.io.FileInputStream;
import java.io.IOException;

import junit.framework.TestCase;
import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.indexer.storage.mongo.StorageFac;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class TestDaemonWriter extends TestCase {

	public void testNotConflict() throws Exception {
		Directory dir = new RAMDirectory();
		Central c = Central.createOrGet(dir);

		IWriter wr1 = c.newDaemonIndexer(new MyKoreanAnalyzer());
		IWriter wr2 = c.newDaemonIndexer(new MyKoreanAnalyzer());

		wr1.begin("usr1");
		String[] names = new String[] { "bleujin", "jin", "hero" };
		for (int i : ListUtil.rangeNum(10)) {
			MyDocument doc = MyDocument.testDocument();
			doc.add(MyField.number("age", RandomUtil.nextInt(60) + 10)).add(MyField.keyword("name", names[RandomUtil.nextInt(3)]));
			wr1.insertDocument(doc);
		}
		
		{   // intercept
			wr2.begin("usr2");
			MyDocument otherDoc = MyDocument.testDocument().add(MyField.number("age", 20)).add(MyField.keyword("name", "jini"));
			wr2.insertDocument(otherDoc);

			wr2.end();
		}
		ISearcher searcher = c.newSearcher();
		assertEquals(1, searcher.searchTest("jini").getTotalCount());
		
		wr1.end();

		searcher = c.newSearcher();
		assertEquals(11, searcher.searchTest("").getTotalCount());
	}

	
	public void testWaitingSession() throws Exception {
		
		Directory dir = new RAMDirectory();
		Central c = Central.createOrGet(dir);

		JobEntry<Boolean> job = new JobEntry<Boolean>() {
			@Override
			public Analyzer getAnalyzer() {
				return new MyKoreanAnalyzer();
			}

			@Override
			public Boolean handle(IWriter writer) throws IOException {
				writer.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "bleujin"))) ;
				return true;
			}

			@Override
			public void onException(Throwable ex) {
			}
		} ;
		
		c.newDaemonHander().addIndexJob(job) ;
		c.newDaemonHander().waitForFlushed() ;
		
		ISearcher searcher = c.newSearcher();
		assertEquals(1, searcher.searchTest("").getTotalCount());
		
	}
	
	
	public void testSpeed() throws Exception {
		Directory dir = new RAMDirectory();
		// Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "test", "stest") ;
		Central c = Central.createOrGet(dir);

		IWriter[] wrs = new IWriter[]{c.newIndexer(new MyKoreanAnalyzer()), c.newDaemonIndexer(new MyKoreanAnalyzer())} ;
		String readString = IOUtil.toString(new FileInputStream("libsource/build_fat.xml"));

		for (IWriter wr : wrs) {
			long start = System.currentTimeMillis() ;
			wr.begin("my") ;
			for (int i : ListUtil.rangeNum(1000)) {
				MyDocument doc = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.text("file", readString)) ;
				wr.insertDocument(doc) ;
			}
			wr.end() ;
			Debug.line(wr, System.currentTimeMillis()- start) ;
		}
		
		// expect (천만 8hour) -> 330 per sec
		// if keyword : 370 per sec
		// if text(3k use MyKoreAnalyzer) : 60 per sec... -_-
		// if text(3k user StandardAnalyzer) : 100 per sec.. -_-)
		// if 3k text & ramdir & mykorean : 200 per sec ... -_-
	}
	
	public void xtestOverCount() throws Exception {
		Directory dir = new RAMDirectory() ;
		Central c = Central.createOrGet(dir);

		IWriter wr = c.newDaemonIndexer(new MyKoreanAnalyzer()) ;

		wr.begin("my") ;
		for (int i : ListUtil.rangeNum(100)) {
			MyDocument doc = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.text("name", "bleujin")) ;
			wr.insertDocument(doc) ;
		}
		wr.end() ;
		
		ISearcher searcher = c.newSearcher() ;
		searcher.searchTest("").debugPrint(Page.ALL) ;
		assertEquals(100, searcher.searchTest("").getTotalCount()) ;
		
	}
	
	
	public void testLength() throws Exception {
		// Directory dir = new RAMDirectory();
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "test", "stest") ;
		Central c = Central.createOrGet(dir);

		IWriter wr = c.newIndexer(new MyKoreanAnalyzer()) ;
		String readString = IOUtil.toString(new FileInputStream("libsource/build_fat.xml")); //3745 byte

		wr.begin("my") ;
		for (int i : ListUtil.rangeNum(1000)) {
			MyDocument doc = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.text("file", readString)) ;
			wr.insertDocument(doc) ;
		}
		wr.end() ;

		long sum = 0 ;
		for(String fileName : dir.listAll()){
			sum += dir.fileLength(fileName) ;
		}
		Debug.line(sum) ; // expect 3,745,000 actual 5,242,565 
	}
	
	
	
	
}
