package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetTracksByArtist extends VoltProcedure{
	
	final SQLStmt findByName = new SQLStmt("SELECT t.code, t.name FROM track t WHERE t.artistCode = ?");
	
	public VoltTable run (String artistCode){
		
		voltQueueSQL(findByName, artistCode);
		
		return voltExecuteSQL(true)[0];
		
	}
}
