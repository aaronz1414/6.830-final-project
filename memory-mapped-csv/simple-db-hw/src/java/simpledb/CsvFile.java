package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CsvFile implements DbFile {
	
	private long lastKnownCsvMod;
	private File file;
	private TupleDesc tupleDesc;
	private CsvPage page;
	
	public CsvFile(File file, TupleDesc td) {
		this.file = file;
		this.tupleDesc = td;
	}

	@Override
	public Page readPage(PageId id) {
		System.out.println("reading page");
		List<Tuple> tuples = new ArrayList<Tuple>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while(line != null) {
				tuples.add(getTupleFromString(line));
				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		lastKnownCsvMod = file.lastModified();
		page = new CsvPage(new CsvPageId(getId(), 0), tuples);
		return page;
	}

	@Override
	public void writePage(Page p) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DbFileIterator iterator(TransactionId tid) {
		if (page == null || isModified()) readPage(null);
		
		return new DbFileIteratorImpl();
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
		return 1;
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
				tuple.setField(i, new StringField(values[i], 64));
			}
		}
		
		return tuple;
	}
	
	private class DbFileIteratorImpl extends AbstractDbFileIterator {
		
		private Iterator<Tuple> iterator;
		private boolean open;
		
		public DbFileIteratorImpl() {
			this.iterator = page.iterator();
			open = false;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			open = true;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			iterator = page.iterator();
		}

		@Override
		protected Tuple readNext() throws DbException, TransactionAbortedException {
			if (iterator.hasNext()) return iterator.next();
			
			return null;
		}
		
	}

}
