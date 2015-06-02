package org.yottabase.lastfm.driver.voltdb.procedure;

import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class InsertListenedTrackRecursive extends VoltProcedure{

	final SQLStmt insert = new SQLStmt(
            "INSERT INTO ListenedTrack (time, trackCode, userCode) VALUES (?, ?, ?);");
	
	public long run (String userCode, Date time, String artistCode, String artistName, String trackCode, String trackName){
		
		voltQueueSQL(insert, EXPECT_SCALAR_MATCH(1), time, trackCode, userCode);
		voltExecuteSQL(true);
		
		return 0;
	}
}
