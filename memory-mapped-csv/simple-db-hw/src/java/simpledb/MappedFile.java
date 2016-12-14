package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

public class MappedFile implements DbFile {
	
	private File file;
	private TupleDesc tupleDesc;
	private MappedByteBuffer buffer;
	private RandomAccessFile raf;
	private FileChannel fc;
	
	public MappedFile(File csv, TupleDesc td) {
		this.file = csv;
		this.tupleDesc = td;
//		this.buffer = getMappedByteBuffer();
	}
	
	private MappedByteBuffer getMappedByteBuffer() {
		try {
			raf = new RandomAccessFile(file, "rw");
			fc = raf.getChannel();
			return fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

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
		return new DbFileIteratorImpl(tid);
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
		return 0;
	}

	@Override
	public boolean isModified() {
		return false;
	}
	
private class DbFileIteratorImpl extends AbstractDbFileIterator {
		
		TransactionId transactionId;
    	int pos;
    	boolean open;

    	public DbFileIteratorImpl(TransactionId tid) {
    		this.transactionId = tid;
    		open = false;
    		pos = 0;
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			open = true;
			buffer = getMappedByteBuffer();
		}
		
		@Override
		public void close() {
			super.close();
			open = false;
			try {
				fc.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			pos = 0;
			buffer.position(pos);
		}
	
		@Override
		protected Tuple readNext() throws DbException, TransactionAbortedException {
			if (!open) return null;
			
			char next = ' ';
			try {
				next = (char) buffer.get();
				pos += 2;
			} catch(BufferUnderflowException e) {
				return null;
			}
			
			StringBuilder sb = new StringBuilder();
			Tuple tuple = new Tuple(tupleDesc);
			int fieldNumber = 0;
			while(next != '\r') {
				if (next == ',') {
					if (tupleDesc.getFieldType(fieldNumber).equals(Type.INT_TYPE)) {
						tuple.setField(fieldNumber, new IntField(Integer.valueOf(sb.toString())));
					} else if (tupleDesc.getFieldType(fieldNumber).equals(Type.STRING_TYPE)) {
						tuple.setField(fieldNumber, new StringField(sb.toString(), 2048));
					}
					fieldNumber++;
					sb = new StringBuilder();
				} else {
					if (next != '\n' && next != '\r') {
						sb.append(next);
					}
				}

				try {
					next = (char) buffer.get();
					pos += 2;
				} catch(BufferUnderflowException e) {
					if (sb.toString().equals("")) return null;
					
					if (tupleDesc.getFieldType(fieldNumber).equals(Type.INT_TYPE)) {
						tuple.setField(fieldNumber, new IntField(Integer.valueOf(sb.toString())));
					} else if (tupleDesc.getFieldType(fieldNumber).equals(Type.STRING_TYPE)) {
						tuple.setField(fieldNumber, new StringField(sb.toString(), 2048));
					}
					
					return tuple;
				}
			}
			
			if (sb.toString().equals("")) return null;
			
			if (tupleDesc.getFieldType(fieldNumber).equals(Type.INT_TYPE)) {
				tuple.setField(fieldNumber, new IntField(Integer.valueOf(sb.toString())));
			} else if (tupleDesc.getFieldType(fieldNumber).equals(Type.STRING_TYPE)) {
				tuple.setField(fieldNumber, new StringField(sb.toString(), 2048));
			}
			
			return tuple;
		}
		
	}

}
