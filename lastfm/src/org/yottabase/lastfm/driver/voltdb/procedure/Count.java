package org.yottabase.lastfm.driver.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Count extends VoltProcedure{

	final SQLStmt countArtists = new SQLStmt("SELECT COUNT(*) 	FROM Artist;");
	final SQLStmt countTracks = new SQLStmt("SELECT COUNT(*) 	FROM Track;");
	final SQLStmt countUsers = new SQLStmt("SELECT COUNT(*) 	FROM User;");
	
	public long run (String tableName){
		
		
		
		switch (tableName) {
			case "Artists":
				voltQueueSQL(countArtists, EXPECT_ZERO_OR_ONE_ROW);
				break;
			
			case "Tracks":
				voltQueueSQL(countTracks, EXPECT_ZERO_OR_ONE_ROW);
				break;
				
			case "Users":
				voltQueueSQL(countUsers, EXPECT_ZERO_OR_ONE_ROW);
				break;
				
			case "Artists+Tracks+Users":
				voltQueueSQL(countArtists, EXPECT_ZERO_OR_ONE_ROW);
				voltQueueSQL(countTracks, EXPECT_ZERO_OR_ONE_ROW);
				voltQueueSQL(countUsers, EXPECT_ZERO_OR_ONE_ROW);
				break;
				
			default:
				throw new VoltAbortException("Count not available");
		
		}
		
		VoltTable[] tablesCount = voltExecuteSQL(true);
		long count = 0;
		
		for (int i = 0; i < tablesCount.length; i++) {
			tablesCount[i].advanceRow();
			count += tablesCount[i].getLong(0);
		}
		
		return count;
	}
}
