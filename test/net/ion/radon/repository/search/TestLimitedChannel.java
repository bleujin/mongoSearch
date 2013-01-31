package net.ion.radon.repository.search;

import junit.framework.TestCase;
import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.nsearcher.search.processor.LimitedChannel;

public class TestLimitedChannel extends TestCase {

	
	public void testCreate() throws Exception {
		LimitedChannel<MessageJob> channel = new LimitedChannel<MessageJob>(5) ;
		
		Cooker[] cookers = new Cooker[3] ;
		for (int i : ListUtil.rangeNum(cookers.length)) {
			cookers[i] = new Cooker(channel, new String[]{"hero","blue","jin"}[i] ) ;
			cookers[i].start() ;
		}
		
		Eater[] eaters = new Eater[1] ;
		for (int i : ListUtil.rangeNum(eaters.length)) {
			eaters[i] = new Eater(channel, new String[]{"novision","iihi","pm1200"}[i] ) ;
			eaters[i].start() ;
		}
		
		new InfinityThread().startNJoin() ;
		
	}
	
}

class Cooker extends Thread {

	private LimitedChannel<MessageJob> channel ;
	
	private String myname ;
	Cooker(LimitedChannel<MessageJob> channel, String myname){
		this.channel = channel ;
		this.myname = myname ;
	}
	
	public void run(){
		int index = 0 ;
		while (true) {
			try {
				Thread.sleep(RandomUtil.nextRandomInt(500, 3000)) ;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			MessageJob mjob = new MessageJob(myname + "' " + (++index));
			channel.addMessage(mjob) ;
			Debug.debug(mjob.getName() + " created") ;
		}
	}
	
}


class Eater extends Thread {
	private LimitedChannel<MessageJob> channel ;
	
	private String myname ;
	Eater(LimitedChannel<MessageJob> channel, String myname){
		this.channel = channel ;
		this.myname = myname ;
	}
	
	public void run(){
		int index = 0 ;
		while (true) {
			try {
				Thread.sleep(RandomUtil.nextRandomInt(500, 3000)) ;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			MessageJob job = channel.pollMessage() ;
			Debug.debug(job.getName() + " eated") ;
		}
	}
	
}


class MessageJob {
	private String name ;
	MessageJob(String name){
		this.name = name ;
	}
	
	public String getName(){
		return name ;
	}
}
