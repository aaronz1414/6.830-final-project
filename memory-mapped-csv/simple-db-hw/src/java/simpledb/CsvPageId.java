package simpledb;

public class CsvPageId implements PageId {
	
	private int tableId;
	private int pageNumber;
	
	public CsvPageId(int tableId, int pageNumber) {
		this.tableId = tableId;
		this.pageNumber = pageNumber;
	}

	@Override
	public int[] serialize() {
		int[] data = new int[2];
		data[0] = getTableId();
		data[1] = getPageNumber();
		
		return data;
	}

	@Override
	public int getTableId() {
		return tableId;
	}

	@Override
	public int getPageNumber() {
		return pageNumber;
	}
	
	/**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        return tableId * 97 + pageNumber * 103;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if (!(o instanceof PageId)) return false;
        
        PageId pageId = (PageId) o;
        
        return tableId == pageId.getTableId() && pageNumber == pageId.getPageNumber();
    }

}
