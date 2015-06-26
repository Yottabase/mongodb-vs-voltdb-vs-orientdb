package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetArtist extends VoltProcedure{

	final SQLStmt findByCode = new SQLStmt("SELECT * FROM artist a WHERE a.code = ?");
	
	final SQLStmt findByName = new SQLStmt("SELECT * FROM artist a WHERE a.name = ?");
	
	public VoltTable run (String mode, String param){
		
		switch (mode) {
			case "CODE":
				voltQueueSQL(findByCode, EXPECT_ZERO_OR_ONE_ROW, param);
				break;
			
			case "NAME":
				voltQueueSQL(findByName, EXPECT_ZERO_OR_ONE_ROW, param);
				break;

		default:
			throw new VoltAbortException("Find method not available");
		}
		
		return voltExecuteSQL(true)[0];
		
	}
}
