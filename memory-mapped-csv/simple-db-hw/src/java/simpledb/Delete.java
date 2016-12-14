package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator child;
    private TransactionId transactionId;
    private boolean hasBeenCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	this.child = child;
        this.transactionId = t;
        this.hasBeenCalled = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (hasBeenCalled) return null;
    	
    	int numberOfDeletions = 0;
    	while (child.hasNext()) {
    		try {
    			Database.getBufferPool().deleteTuple(transactionId, child.next());
    			numberOfDeletions++;
    		} catch (NoSuchElementException | IOException e) { e.printStackTrace(); }
    	}
        
    	hasBeenCalled = true;
        Tuple response = new Tuple(getTupleDesc());
        response.setField(0, new IntField(numberOfDeletions));
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
