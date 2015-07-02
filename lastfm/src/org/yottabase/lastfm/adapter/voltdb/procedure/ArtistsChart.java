package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;


public class ArtistsChart extends VoltProcedure{
	
	final String query = 
			"SELECT TOP ? a.code, a.name, SUM(tc.num) as num " +
			"FROM trackChart tc LEFT JOIN track t ON (tc.code=t.code) " +
			"LEFT JOIN artist a ON (t.artistCode = a.code) " +
			"GROUP BY a.code, a.name ORDER BY num ";
	
	final SQLStmt chartAsc = new SQLStmt(query + " ASC");
	final SQLStmt chartDesc = new SQLStmt( query + " DESC");
	
	public VoltTable run (int n, String order){
		
		switch (order) {
			case "ASC":
				voltQueueSQL(chartAsc, n);
				break;
				
			case "DESC":
				voltQueueSQL(chartDesc, n);
				break;
	
			default:
				throw new VoltAbortException("Order not available");
		}
		
		voltQueueSQL(chartAsc, n);
		
		return voltExecuteSQL(true)[0];
		
	}
}
