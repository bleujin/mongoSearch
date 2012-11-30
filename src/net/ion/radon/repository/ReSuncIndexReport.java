package net.ion.radon.repository;

import java.util.Calendar;

import net.ion.framework.util.HttpUtils;
import net.ion.framework.util.StringUtil;

public class ReSuncIndexReport {
	public static final ReSuncIndexReport NONE = new ReSuncIndexReport(true);

	private StringBuffer progress;
	private boolean empty = false;
	private boolean ended = false;

	public static final ReSuncIndexReport create() {
		return new ReSuncIndexReport(false);
	}

	private ReSuncIndexReport(boolean empty) {
		this.progress = new StringBuffer();
		this.empty = empty;
	}

	public String getHtmlCurrentInfo() {
		String msg = progress.toString();
		progress.delete(0, msg.length());
		if(StringUtil.isEmpty(msg)){
			return " ";
		} 
		return HttpUtils.replaceNewLine(HttpUtils.filterHTML(msg)).replace('\\', '/');
	}

    public void addInfoLineWithTime(String message){
        message = "[" + getTime() + "] " + message;
        addInfoLine(message);
    }

    public void addInfoWithTime(String message){
        message = "[" + getTime() + "] " + message;
        addInfo(message);
    }
    
	public void addInfoLine(String message) {
		if (!empty) {
			progress.append(message + "\n");
		}
		System.out.println(message);
	}

	public void addInfo(String message) {
		if (!empty) {
			progress.append(message);
		}
		System.out.print(message);
	}

	public boolean isEnded() {
		return ended;
	}

	public void setEnded(boolean ended) {
		this.ended = ended;
	}
	
    private String getTime(){
        return Calendar.getInstance().getTime().toString();
    }
}
