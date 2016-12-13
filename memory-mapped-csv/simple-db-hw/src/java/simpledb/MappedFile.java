package simpledb;

import java.io.IOException;
import java.util.ArrayList;

public class MappedFile implements DbFile {

	@Override
	public Page readPage(PageId id) {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public TupleDesc getTupleDesc() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int numPages() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isModified() {
		// TODO Auto-generated method stub
		return false;
	}

}
