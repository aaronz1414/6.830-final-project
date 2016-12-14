package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File file;
	private File csv;
	private TupleDesc tupleDesc;
	private boolean isCsvBacked;
	private long lastKnownCsvMod;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	System.out.println("heap file");
    	tupleDesc = td;
    	csv = null;
    	isCsvBacked = false;
    	
    	if (f.getName().contains(".csv")) {
    		csv = f;
    		isCsvBacked = true;
    		reloadFileFromCsv();
    	} else {
    		file = f;
    	}
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }
    
    public boolean isModified() {
    	return isCsvBacked && csv.lastModified() > lastKnownCsvMod;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	if (isCsvBacked && csv.lastModified() > lastKnownCsvMod) {
    		reloadFileFromCsv();
    	}
    	
//    	System.out.println("reading page");
    	
    	byte[] data = new byte[BufferPool.getPageSize()];
    	
    	try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
			randomAccessFile.seek(pid.getPageNumber() * BufferPool.getPageSize());
			randomAccessFile.read(data);
			randomAccessFile.close();
			
			return new HeapPage(new HeapPageId(getId(), pid.getPageNumber()), data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
		randomAccessFile.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
		randomAccessFile.write(page.getPageData());
		randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	int currentPageNumber = 0;
    	HeapPage currentPage = (HeapPage) Database.getBufferPool()
				.getPage(tid, new HeapPageId(getId(), currentPageNumber), Permissions.READ_ONLY);
    	while (currentPage.getNumEmptySlots() == 0 && currentPageNumber < numPages()) {
    		Database.getBufferPool().releasePage(tid, new HeapPageId(getId(), currentPageNumber));
    		currentPageNumber++;
    		currentPage = (HeapPage) Database.getBufferPool()
    				.getPage(tid, new HeapPageId(getId(), currentPageNumber), Permissions.READ_ONLY);
    	}
    	
    	currentPage = (HeapPage) Database.getBufferPool()
				.getPage(tid, new HeapPageId(getId(), currentPageNumber), Permissions.READ_WRITE);
    	currentPage.insertTuple(t);
    	
    	if (currentPageNumber >= numPages()) {
    		writePage(currentPage);
    	}
    	
    	ArrayList<Page> pages = new ArrayList<>();
    	pages.add(currentPage);
    	return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	HeapPage page =
    			(HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	
    	page.deleteTuple(t);
    	
    	ArrayList<Page> pages = new ArrayList<>();
    	pages.add(page);
    	return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new DbFileIteratorImpl(tid);
    }
    
    private void reloadFileFromCsv() {
    	file = convert(csv);
		lastKnownCsvMod = csv.lastModified();
    }
    
    private File convert(File sourceTxtFile) {
		try {
            File targetDatFile=new File(sourceTxtFile.getName().replaceAll(".csv", ".dat"));
//            System.out.println("num fields: " + tupleDesc.numFields());
            Type[] ts = new Type[tupleDesc.numFields()];
            char fieldSeparator=',';

            for (int i=0;i<tupleDesc.numFields();i++)
                ts[i]=tupleDesc.getFieldType(i);

            HeapFileEncoder.convert(sourceTxtFile,targetDatFile,
                        BufferPool.getPageSize(),tupleDesc.numFields(),ts,fieldSeparator);
            return targetDatFile;

        } catch (IOException e) {
                throw new RuntimeException(e);
        }
		
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
			while(!currentTupleIterator.hasNext() && nextPageNumber <= numPages() - 1) {
				HeapPage page = (HeapPage) Database.getBufferPool()
						.getPage(transactionId, new HeapPageId(getId(), nextPageNumber), Permissions.READ_ONLY);

				nextPageNumber++;
				currentTupleIterator = page.iterator();
			}
		}
    	
    }

}

