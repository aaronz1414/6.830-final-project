package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator child;
    private TransactionId transactionId;
    private int tableId;
    private boolean hasBeenCalled;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.child = child;
        this.transactionId = t;
        this.tableId = tableid;
        this.hasBeenCalled = false;
        
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
    		throw new DbException("child TupleDesc must equal table TupleDesc");
    	}
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {null});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        hasBeenCalled = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (hasBeenCalled) return null;
    	
    	int numberOfInsertions = 0;
    	while (child.hasNext()) {
    		try {
    			Database.getBufferPool().insertTuple(transactionId, tableId, child.next());
    			numberOfInsertions++;
    		} catch (NoSuchElementException | IOException e) { e.printStackTrace(); }
    	}
        
    	hasBeenCalled = true;
        Tuple response = new Tuple(getTupleDesc());
        response.setField(0, new IntField(numberOfInsertions));
        return response;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}