package org.yottabase.lastfm.driver.voltdb.procedure;

import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class InsertListenedTrackRecursive extends VoltProcedure{

	final SQLStmt insertListenedTrack = new SQLStmt("INSERT INTO ListenedTrack (time, trackCode, userCode) VALUES (?, ?, ?);");
	final SQLStmt insertTrack = new SQLStmt("INSERT INTO Track (code, name, artistCode) VALUES (?, ?, ?);");
	final SQLStmt insertArtist = new SQLStmt("INSERT INTO Artist (code, name) VALUES (?, ?);");
	
	final SQLStmt selectTrack = new SQLStmt("SELECT * FROM Track WHERE Code = ? ;");
	final SQLStmt selectArtist = new SQLStmt("SELECT * FROM Artist WHERE Code = ? ;");
	
	public long run (String userCode, Date time, String artistCode, String artistName, String trackCode, String trackName){
		
		boolean trackExists, artistExists;
		
		voltQueueSQL(selectArtist, EXPECT_ZERO_OR_ONE_ROW, artistCode);
		artistExists = (voltExecuteSQL()[0].getRowCount() > 0 );
		
		voltQueueSQL(selectTrack, EXPECT_ZERO_OR_ONE_ROW, trackCode);
		trackExists = (voltExecuteSQL()[0].getRowCount() > 0 );
		
		if(! artistExists){
			voltQueueSQL(insertArtist, EXPECT_SCALAR_MATCH(1), artistCode, artistName);
		}
		
		if(! trackExists){
			voltQueueSQL(insertTrack, EXPECT_SCALAR_MATCH(1), trackCode, trackName, artistCode);
		}
		
		voltQueueSQL(insertListenedTrack, EXPECT_SCALAR_MATCH(1), time, trackCode, userCode);
		voltExecuteSQL(true);
		
		return 0;
	}
}
