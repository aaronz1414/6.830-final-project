package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    private List<TDItem> tdItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	tdItems = new ArrayList<>();
    	
        assert(typeAr.length == fieldAr.length);
        
        for (int i = 0; i < typeAr.length; i++) {
        	tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	tdItems = new ArrayList<>();
    	
    	for (int i = 0; i < typeAr.length; i++) {
        	tdItems.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tdItems.size(); i++) {
        	if (tdItems.get(i).fieldName == null && name == null) {
        		return i;
        	} else if (name != null && name.equals(tdItems.get(i).fieldName)) {
        		return i;
        	}
        }
        
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        
        for (TDItem item : tdItems) {
        	size += item.fieldType.getLen();
        }
        
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int numFields = td1.numFields() + td2.numFields();
        Type[] types = new Type[numFields];
        String[] names = new String[numFields];
        
        for (int i = 0; i < td1.numFields(); i++) {
        	types[i] = td1.getFieldType(i);
        	names[i] = td1.getFieldName(i);
        }
        
        for (int i = 0; i < td2.numFields(); i++) {
        	types[td1.numFields() + i] = td2.getFieldType(i);
        	names[td1.numFields() + i] = td2.getFieldName(i);
        }
        
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) return false;
        
        TupleDesc tupleDesc = (TupleDesc) o;
        
        if (getSize() != tupleDesc.getSize() || numFields() != tupleDesc.numFields()) return false;
        
        for (int i = 0; i < numFields(); i++) {
        	if (!tdItems.get(i).fieldType.equals(tupleDesc.getFieldType(i))) {
        		return false;
        	}
        }
        
        return true;
    }

    public int hashCode() {
    	int sum = 0;
    	
    	for (TDItem item : tdItems) {
    		sum += item.fieldType.getLen() * 97;
    	}
        
    	return sum;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	StringBuilder stringBuilder = new StringBuilder();
    	
        for (TDItem item : tdItems.subList(0, numFields() - 2)) {
        	stringBuilder.append(item.fieldType + "(" + item.fieldName + "), ");
        }
        
        TDItem lastItem = tdItems.get(numFields() - 1);
        stringBuilder.append(lastItem.fieldType + "(" + lastItem.fieldName + ")");
        
        return stringBuilder.toString();
    }
}
