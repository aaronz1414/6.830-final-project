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

}
