package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate predicate;
    private DbIterator[] children;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        children = new DbIterator[1];
        children[0] = child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
       return children[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        children[0].open();
    }

    public void close() {
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        children[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	if (!children[0].hasNext()) return null;
    	Tuple current = children[0].next();
    	
        while (children[0].hasNext() && !predicate.filter(current)) {
        	current = children[0].next();
        }
        
        if (predicate.filter(current)) return current;
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.children[0] = children[0];
    }

}
