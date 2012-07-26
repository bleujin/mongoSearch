package net.ion.isearcher.directory;

import org.apache.lucene.analysis.kr.KoreanAnalyzer;
import org.apache.lucene.store.Directory;

import net.ion.framework.db.Page;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.ISearcher;
import net.ion.isearcher.indexer.storage.mongo.StorageFac;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.ISearchRequest;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.isearcher.searcher.SearchRequest;
import junit.framework.TestCase;

public class TestMongoSegment extends TestCase {

	public void testCase1() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = Central.createOrGet(dir);
		
		IWriter writer = c.newDaemonIndexer(new KoreanAnalyzer()) ;
		writer.begin("seg") ;
		for (int i = 0; i < 30 ; i++) {
			MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin").number("index", i));
			writer.insertDocument(doc) ;
		}
		
		writer.end() ;
	}


	public void testCase3() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = Central.createOrGet(dir);
		
		IWriter writer = c.newIndexer(new KoreanAnalyzer()) ;
		writer.begin("seg") ;
		for (int i = 0; i < 30 ; i++) {
			MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin").number("index", i));
			writer.insertDocument(doc) ;
		}
		writer.end() ;
	}
	
	public void testCase2() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = Central.createOrGet(dir);
		
		IWriter writer = c.newDaemonIndexer(new KoreanAnalyzer()) ;
		for (int i = 0; i < 30 ; i++) {
			writer.begin("seg") ;
			MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin").number("index", i));
			writer.insertDocument(doc) ;
			writer.end() ;
		}
		
	}
	
	
	public void xtestAppend() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "seg");
		Central c = Central.createOrGet(dir);
		IWriter writer = c.newDaemonIndexer(new KoreanAnalyzer()) ;
		for (int i = 0; i < 30 ; i++) {
			writer.begin("seg") ;
			MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin").number("index", i));
			writer.insertDocument(doc) ;
			writer.end() ;
		}
	}
	
	public void xtestSearch() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "seg");
		Central c = Central.createOrGet(dir);
		ISearcher search = c.newSearcher() ;
		ISearchRequest req = SearchRequest.create("", null, new MyKoreanAnalyzer()) ;
		req.setPage(Page.ALL) ;
		search.search(req).debugPrint(Page.ALL) ;
	}
	
	
	public void testCase4() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = Central.createOrGet(dir);
		
		IWriter writer = c.newIndexer(new KoreanAnalyzer()) ;
		for (int i = 0; i < 30 ; i++) {
			writer.begin("seg") ;
			MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin").number("index", i));
			writer.insertDocument(doc) ;
			writer.end() ;
		}
	}
	

}
