package net.ion.radon.repository.search;

import junit.framework.TestCase;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.SearchRepositoryCentral;
import net.ion.radon.repository.Session;

public class TestIndexMultiThread extends TestCase {

	public void testSessionRun() throws Exception {
		RepositoryCentral rc = RepositoryCentral.testCreate() ;
		
		Creater[] creaters = new Creater[10] ;
		for (int i : ListUtil.rangeNum(10)) {
			Session ss = rc.testLogin("working") ;
			creaters[i] = new Creater(ss, "n" + i + "m") ;
			creaters[i].start() ;
		}
		
		new InfinityThread().startNJoin() ;
	}

	public void testSearchRun() throws Exception {
		SearchRepositoryCentral src = SearchRepositoryCentral.testCreate() ;
		
		Creater[] creaters = new Creater[10] ;
		for (int i : ListUtil.rangeNum(10)) {
			creaters[i] = new Creater(src.testLogin("working"), "n" + i + "m") ;
			creaters[i].start() ;
		}
		
		new InfinityThread().startNJoin() ;
	}
}

class Creater extends Thread {
	
	private Session session ;
	Creater(Session session, String name){
		super(name) ;
		this.session = session ; 
	}
	
	public void run(){
	
		int index = 0 ;
		while(true){
			try {
				Thread.sleep(RandomUtil.nextRandomInt(50, 100)) ;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			session.newNode().put("create", getName()).put("index", index++).getSession().commit() ;
			System.out.print('.') ;
		}
		
	}
}
