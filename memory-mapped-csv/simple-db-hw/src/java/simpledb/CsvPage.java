package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CsvPage implements Page {

	List<Tuple> oldData;
    private final Byte oldDataLock=new Byte((byte)0);
    private CsvPageId pid;
    private List<Tuple> tuples;
    private TransactionId lastTransaction;
    private boolean isDirty;
    
    public CsvPage(CsvPageId pid, List<Tuple> tuples) {
    	this.pid = pid;
    	this.tuples = tuples;
    	this.isDirty = false;
    }
	
	@Override
	public PageId getId() {
		return pid;
	}

	@Override
	public TransactionId isDirty() {
		if (isDirty) return lastTransaction;
		return null;
	}

	@Override
	public void markDirty(boolean dirty, TransactionId tid) {
		isDirty = true;
		lastTransaction = tid;
	}

	@Override
	public byte[] getPageData() {
		return null;
	}

	@Override
	public Page getBeforeImage() {
		List<Tuple> oldDataRef = null;
        synchronized(oldDataLock)
        {
            oldDataRef = oldData;
        }
        return new CsvPage(pid, oldDataRef);
	}

	@Override
	public void setBeforeImage() {
		synchronized(oldDataLock)
        {
        oldData = new ArrayList<Tuple>(tuples);
        }
	}
	
	public Iterator<Tuple> iterator() {
		return tuples.iterator();
	}

}
