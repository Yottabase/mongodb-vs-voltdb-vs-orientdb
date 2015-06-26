package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class UsersStats extends VoltProcedure{

	final SQLStmt byCountry = new SQLStmt("SELECT u.country, count(*) as num FROM user u WHERE u.country IS NOT NULL GROUP BY u.country");
	final SQLStmt byCountryAndGender = new SQLStmt("SELECT u.country, u.gender, count(*) as num FROM user u WHERE u.country IS NOT NULL AND u.gender IS NOT NULL GROUP BY u.country, u.gender");
	
	public VoltTable run (String tableName){
		
		switch (tableName) {
			case "Country":
				voltQueueSQL(byCountry);
				break;
			
			case "Country+Gender":
				voltQueueSQL(byCountryAndGender);
				break;
			
			default:
				throw new VoltAbortException("Group not available");
		}
		
		return voltExecuteSQL(true)[0];
	}
}
