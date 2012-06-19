package net.ion.isearcher.directory;

import java.io.IOException;
import java.util.HashMap;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.isearcher.ISTestCase;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.DaemonIndexer;
import net.ion.isearcher.impl.ISearcher;
import net.ion.isearcher.impl.JobEntry;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.ISearchRequest;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.isearcher.searcher.SearchRequest;
import net.ion.isearcher.searcher.processor.StdOutProcessor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.mongodb.util.MyAsserts;

import junit.framework.TestCase;

public class TestDirectory extends ISTestCase{

	public void testCopy() throws Exception {
		Directory source = new RAMDirectory() ;
		Directory target = new RAMDirectory() ;
		
		Central cs = Central.createOrGet(source) ;
		Central ct = Central.createOrGet(target) ;
		
		DaemonIndexer dsource = cs.newDaemonHander() ;
		dsource.addIndexJob(new MyIndexJob("hero")) ;
		dsource.waitForFlushed() ;

		DaemonIndexer dtarget = cs.newDaemonHander() ;
		dtarget.addIndexJob(new MyIndexJob("bleujin")) ;
		dtarget.waitForFlushed() ;
		
		ct.copyFrom(new MyKoreanAnalyzer(), cs.getDir()) ;
		
		assertEquals(10, ct.newSearcher().searchTest("").getTotalCount()) ;
	}
	
	
	public void testMerge() throws Exception {

		Directory source = new RAMDirectory() ;
		Directory target = new RAMDirectory() ;
		
		Central cs = Central.createOrGet(source) ;
		Central ct = Central.createOrGet(target) ;
		
		MyDocument[] docs = new MyDocument[5] ;
		for (int i = 0; i < docs.length; i++) {
			docs[i] = MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.keyword("name", "jin")) ;
		}
		
		Analyzer analyzer = new MyKoreanAnalyzer();
		addDocTo(cs, docs, analyzer);

		ct.copyFrom(analyzer, source) ;

		cs.newSearcher().searchTest("").debugPrint(Page.ALL) ;
		Debug.line() ;

		for (int i = 0; i < docs.length; i++) {
			docs[i].add(MyField.keyword("newname", "hero")) ;
		}
		
		addDocTo(cs, docs, analyzer);
		cs.newSearcher().searchTest("").debugPrint(Page.ALL) ;
		Debug.line() ;

		
		ct.copyFrom(analyzer, source) ;
		ct.newSearcher().searchTest("").debugPrint(Page.ALL) ;
		
		
	}

	public void testDirCopy() throws Exception {
		Directory dir1 = writeDocument() ;
		
		Central cen1 = Central.createOrGet(dir1) ;
		Central cen2 = write2Dir() ;

		ISearcher searcher = cen1.newSearcher() ;
		searcher.addPostListener(new StdOutProcessor()) ;
		ISearchRequest req = SearchRequest.test("bleujin") ;
		assertEquals(6, searcher.search(req).getTotalCount()) ;

		
		cen1.copyFrom(new MyKoreanAnalyzer(), cen2.getDir()) ;
		
		
		searcher = cen1.newSearcher() ;
		
		searcher.searchTest("").debugPrint(Page.ALL) ;
		
		
		assertEquals(9, searcher.search(req).getTotalCount()) ;
		
		searcher = Central.createOrGet(dir1).newSearcher() ;
		searcher.addPostListener(new StdOutProcessor()) ;
		req = SearchRequest.test("bleujin") ;
		assertEquals(9, searcher.search(req).getTotalCount()) ;
		
		
		searcher = cen2.newSearcher() ;
		searcher.addPostListener(new StdOutProcessor()) ;
		req = SearchRequest.test("bleujin") ;
		assertEquals(3, searcher.search(req).getTotalCount()) ;
		
		
	}


	private void addDocTo(Central cs, MyDocument[] docs, Analyzer analyzer) throws LockObtainFailedException, IOException {
		IWriter iwcs = cs.newIndexer(analyzer) ;
		iwcs.begin("my") ;
		for (int i = 0; i < docs.length; i++) {
			iwcs.updateDocument(docs[i]) ;
		}
		iwcs.end() ;
	}
	
	
	public void testLockId() throws Exception {
		Directory d1 = new RAMDirectory() ;
		Directory d2 = new RAMDirectory() ;
		
		Debug.line(d1.getLockID(), d2.getLockID()) ;
		
		
	}
	
	private Central write2Dir() throws LockObtainFailedException, IOException {
		Directory dir = new RAMDirectory() ;
		Central cen = Central.createOrGet(dir) ;
		IWriter writer = cen.testIndexer(new CJKAnalyzer(Version.LUCENE_CURRENT)) ;
		
		
		writer.begin("test") ;
		for (int i = 0; i < 3 ; i++) {
			MyDocument doc = MyDocument.testDocument() ;
			doc.add(MyField.text("name", "bleujin")) ;
			writer.insertDocument(doc) ;
		}
		writer.end() ;
		return cen ;
	}

	
	public void testAppendFrom() throws Exception {
		
		Directory tempDir = new RAMDirectory() ;
		Central tempCen = Central.createOrGet(tempDir) ;
		IWriter tempWriter = tempCen.newIndexer(new CJKAnalyzer(Version.LUCENE_36)) ;
		tempWriter.begin("temp") ;
		MyDocument doc = MyDocument.testDocument().add(MyField.text("name", "bleujin"));
		tempWriter.insertDocument(doc) ;
		tempWriter.end() ;
		
		
		Directory dir = new RAMDirectory() ;
		Central cen = Central.createOrGet(dir) ;
		MyKoreanAnalyzer analyzer = new MyKoreanAnalyzer();
		IWriter writer = cen.newIndexer(analyzer) ;
		writer.begin("my") ;
		// writer.insertDocument(MyDocument.testDocument().add(MyField.text("name", "hero"))) ;
		doc.add(MyField.text("name", "hero")) ;
		writer.insertDocument(doc) ;
		writer.appendFrom(tempDir) ;

		writer.end() ;
		
		ISearcher searcher = cen.newSearcher() ;
		assertEquals(2, searcher.searchTest("").getTotalCount()) ;
	}
	
}

class MyIndexJob extends JobEntry<Boolean> {
	private Analyzer anal = new MyKoreanAnalyzer() ;
	private String name ;
	MyIndexJob(String name){
		this.name = name ;
	}
	
	public Analyzer getAnalyzer() {
		return anal;
	}

	public Boolean handle(IWriter writer) throws IOException {
		for (int i : ListUtil.rangeNum(5)) {
			writer.insertDocument(MyDocument.testDocument().add(MyField.number("index", i)).add(MyField.keyword("name", name))) ;
		}
		return true;
	}

	public void onException(Throwable ex) {
	}
}
