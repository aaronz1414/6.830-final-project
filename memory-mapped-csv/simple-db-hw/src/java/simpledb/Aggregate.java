package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    DbIterator child;
    int aggregateField;
    int groupByField;
    Aggregator.Op operator;
    Aggregator aggregator;
    DbIterator aggregateIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		this.child = child;
		this.aggregateField = afield;
		this.groupByField = Aggregator.NO_GROUPING;
		this.operator = aop;
		this.aggregateIterator = null;
		
		Type groupByFieldType = null;
		if (gfield != -1) {
			groupByFieldType = child.getTupleDesc().getFieldType(gfield);
			groupByField = gfield;
		}
		
		if (Type.INT_TYPE.equals(child.getTupleDesc().getFieldType(afield))) {
			this.aggregator = new IntegerAggregator(gfield, groupByFieldType, afield, aop);
		} else if (Type.STRING_TYPE.equals(child.getTupleDesc().getFieldType(afield))) {
			this.aggregator = new StringAggregator(gfield, groupByFieldType, afield, aop);
		}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	return groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
		if (groupByField == -1) return null;
		
		return child.getTupleDesc().getFieldName(groupByField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		return operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	if (aggregateIterator == null) {
    		child.open();
    		while (child.hasNext()) {
    			aggregator.mergeTupleIntoGroup(child.next());
    		}
    		child.close();
    		
    		aggregateIterator = aggregator.iterator();
    	}
    	
    	super.open();
		aggregateIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (aggregateIterator.hasNext()) {
    		return aggregateIterator.next();
    	}
		
    	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
		aggregateIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc tupleDesc = new TupleDesc(
    			new Type[] {child.getTupleDesc().getFieldType(aggregateField)},
    			new String[] {aggregateFieldName()});
        
        if (groupByField != Aggregator.NO_GROUPING) {
        	tupleDesc = new TupleDesc(
        			new Type[] {child.getTupleDesc().getFieldType(groupByField),
        					child.getTupleDesc().getFieldType(aggregateField)},
        			new String[] {groupFieldName(), aggregateFieldName()});
        }
        
        return tupleDesc;
    }

    public void close() {
    	super.close();
		aggregateIterator.close();
    }

    @Override
    public DbIterator[] getChildren() {
		return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
		child = children[0];
    }
    
}
