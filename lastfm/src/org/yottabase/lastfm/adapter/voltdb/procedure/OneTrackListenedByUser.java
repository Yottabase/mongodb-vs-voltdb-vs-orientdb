package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class OneTrackListenedByUser extends VoltProcedure{
	
	final SQLStmt findByName = new SQLStmt("SELECT TOP 1 t.code, t.name FROM ListenedTrack lt LEFT JOIN Track t ON (lt.trackCode = t.code) WHERE lt.userCode = ?");
	
	public VoltTable run (String artistCode){
		
		voltQueueSQL(findByName, artistCode);
		
		return voltExecuteSQL(true)[0];
		
	}
}
