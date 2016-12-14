package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

public class TestScanCsv {

	public static void main(String[] args) {
		
		// construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);
        
        String filename = args[0];
        File csvFile=new File(filename);
        
        int scansPerCsvUpdate = Integer.valueOf(args[1]);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        CsvFile table1 = new CsvFile(csvFile, descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        
        
        long total = 0;
        
        try {
            // and run it
        	
        	double totalOfAvg = 0;
        	
        	for (int i = 0; i < 10; i++) {
        		
        		BufferedReader br = new BufferedReader(new FileReader(csvFile));
        		File tmp = new File("tmp");
        		
        		PrintWriter pw = new PrintWriter(tmp);
        		pw.println(i + "," + i + "," + i);
        		br.readLine();
        		String line = br.readLine();
        		while(line != null) {
        			pw.println(line);
        			line = br.readLine();
        		}
        		pw.flush();
        		pw.close();
        		br.close();
        		
        		PrintWriter pw2 = new PrintWriter(new FileWriter(csvFile, false));
        		BufferedReader br2 = new BufferedReader(new FileReader(tmp));
        		String line2 = br2.readLine();
        		while(line2 != null) {
        			pw2.println(line2);
        			line2 = br2.readLine();
        		}
        		pw2.flush();
        		pw2.close();
        		br2.close();
        		
        		tmp.delete();
        		
        		total = 0;
                
        		for (int j = 0; j < scansPerCsvUpdate; j++) {
        			long start = System.currentTimeMillis();
        			TransactionId tid = new TransactionId();
                    SeqScan f = new SeqScan(tid, table1.getId());
        			f.open();
                    while (f.hasNext()) {
                        Tuple tup = f.next();
//                        System.out.println(tup);
                    }
                    f.close();
                    long time = System.currentTimeMillis() - start;
                    System.out.println("Trial " + i + ": " + time);
            		total += time;
            		Database.getBufferPool().transactionComplete(tid);
        		}
        		
        		System.out.println("Total: " + total);
        		double avg = ((total * 1.0) / scansPerCsvUpdate);
            	System.out.println("Average: " + avg + "\n");
        		
            	totalOfAvg += avg;
        		
        	}
        	
        	System.out.println("Average of averages: " + ((totalOfAvg * 1.0) / 10));
        	
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
		
		
	}

}
