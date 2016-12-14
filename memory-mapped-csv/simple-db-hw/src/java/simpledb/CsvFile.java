package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CsvFile implements DbFile {
	
	private long lastKnownCsvMod;
	private File file;
	private TupleDesc tupleDesc;
	private Set<PageId> pages;
	private boolean stopReading;
	private int rowsInCsv;
	
	public CsvFile(File file, TupleDesc td) {
		this.file = file;
		this.tupleDesc = td;
		this.pages = new HashSet<>();
		this.stopReading = false;
		this.rowsInCsv = getNumRowsInCsv();
	}

	@Override
	public Page readPage(PageId id) {
//		System.out.println("reading page");
		List<Tuple> tuples = new ArrayList<Tuple>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			for (int i = 0; i < id.getPageNumber() * getNumTuplesPerPage(); i++) {
				br.readLine();
			}
			
			String line = br.readLine();
			int numTuplesRead = 0;
			while(line != null && numTuplesRead < getNumTuplesPerPage()) {
				tuples.add(getTupleFromString(line));
//				System.out.println(line);
				numTuplesRead++;
				line = br.readLine();
			}
			if (line == null) stopReading = true;
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		lastKnownCsvMod = file.lastModified();
		CsvPageId pageId = new CsvPageId(getId(), id.getPageNumber());
		pages.add(pageId);
//		System.out.println(tuples.size());
		return new CsvPage(pageId, tuples);
	}

	@Override
	public void writePage(Page p) throws IOException {
		PrintWriter pw = new PrintWriter(new FileWriter(file, true));
		Iterator<Tuple> iterator = ((CsvPage) p).iterator();
		
		while(iterator.hasNext()) {
			pw.println(tupleToString(iterator.next()));
		}
		pw.flush();
		pw.close();
	}

	@Override
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		int currentPageNumber = 0;
    	CsvPage currentPage = (CsvPage) Database.getBufferPool()
				.getPage(tid, new CsvPageId(getId(), currentPageNumber), Permissions.READ_ONLY);
    	while (currentPage.numTuples() >= getNumTuplesPerPage() && currentPageNumber < numPages()) {
    		Database.getBufferPool().releasePage(tid, new HeapPageId(getId(), currentPageNumber));
    		currentPageNumber++;
    		currentPage = (CsvPage) Database.getBufferPool()
    				.getPage(tid, new CsvPageId(getId(), currentPageNumber), Permissions.READ_ONLY);
    	}
    	
    	currentPage = (CsvPage) Database.getBufferPool()
				.getPage(tid, new CsvPageId(getId(), currentPageNumber), Permissions.READ_WRITE);
    	currentPage.insertTuple(t);
    	
    	// force tuple to disk to stay in sync with CSV file
    	PrintWriter pw = new PrintWriter(new FileWriter(file, true));
		pw.println(tupleToString(t));
		pw.flush();
		pw.close();
    	
    	rowsInCsv++;
    	ArrayList<Page> pages = new ArrayList<>();
    	pages.add(currentPage);
    	return pages;
	}

	@Override
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DbFileIterator iterator(TransactionId tid) {
		if (isModified()) {
			for(PageId pid : pages) {
				Database.getBufferPool().discardPage(pid);
			}
			pages = new HashSet<>();
			lastKnownCsvMod = file.lastModified();
			rowsInCsv = getNumRowsInCsv();
//			System.out.println("is modified");
		}
		
		return new DbFileIteratorImpl(tid);
	}
	
	private int getNumRowsInCsv() {
		int total = 0;
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while(br.readLine() != null) {
				total++;
			}
		} catch (IOException e) {}
		
		return total;
	}

	@Override
	public int getId() {
		return file.getAbsoluteFile().hashCode();
	}

	@Override
	public TupleDesc getTupleDesc() {
		return tupleDesc;
	}

	@Override
	public int numPages() {
		return ((rowsInCsv - 1) / getNumTuplesPerPage()) + 1;
	}
	
	@Override
	public boolean isModified() {
		return file.lastModified() > lastKnownCsvMod;
	}
	
	private Tuple getTupleFromString(String line) {
		String[] values = line.split(",");
		Tuple tuple = new Tuple(tupleDesc);
		for(int i = 0; i < values.length; i++) {
			if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
				tuple.setField(i, new IntField(Integer.valueOf(values[i])));
			} else if(tupleDesc.getFieldType(i).equals(Type.STRING_TYPE)) {
				tuple.setField(i, new StringField(values[i], 2048));
			}
		}
		
		return tuple;
	}
	
	private String tupleToString(Tuple t) {
		StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i < tupleDesc.numFields(); i++) {
			if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
				sb.append(((IntField) t.getField(i)).getValue());
			} else if (tupleDesc.getFieldType(i).equals(Type.STRING_TYPE)) {
				sb.append(((StringField) t.getField(i)).getValue());
			}
			
			if (i < tupleDesc.numFields() - 1) sb.append(",");
		}
		
		return sb.toString();
	}
	
	/** Retrieve the number of tuples on this page.
    @return the number of tuples on this page
	*/
	private int getNumTuplesPerPage() {        
	    return (int) Math.floor((BufferPool.getPageSize()*8) / (tupleDesc.getSize() * 8 + 1));
	}
	
	private class DbFileIteratorImpl extends AbstractDbFileIterator {
		
		TransactionId transactionId;
    	int nextPageNumber;
    	Iterator<Tuple> currentTupleIterator;
    	
    	boolean open;

    	public DbFileIteratorImpl(TransactionId tid) {
    		this.transactionId = tid;
    		this.nextPageNumber = 0;
    		currentTupleIterator = (new ArrayList<Tuple>()).iterator();
    		open = false;
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			open = true;
			stopReading = false;
		}
		
		@Override
		public void close() {
			super.close();
			open = false;
		}
	
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			nextPageNumber = 0;
			currentTupleIterator = (new ArrayList<Tuple>()).iterator();
			stopReading = false;
			moveToNextPopulatedPage();
		}
	
		@Override
		protected Tuple readNext() throws DbException, TransactionAbortedException {
			if (!open) return null;

			if (currentTupleIterator.hasNext()) {
				Tuple next = currentTupleIterator.next();
				return next;
			}
			
			moveToNextPopulatedPage();
			if (currentTupleIterator.hasNext()) {
				Tuple next = currentTupleIterator.next();
				return next;
			}
			
			return null;
		}
		
		private void moveToNextPopulatedPage() throws TransactionAbortedException, DbException {
			while(!currentTupleIterator.hasNext() && nextPageNumber < numPages()) {
				CsvPage page = (CsvPage) Database.getBufferPool()
						.getPage(transactionId, new CsvPageId(getId(), nextPageNumber), Permissions.READ_ONLY);

				nextPageNumber++;
				currentTupleIterator = page.iterator();
			}
		}
		
	}

}
