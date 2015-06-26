package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;


public class UsersByAgeRange extends VoltProcedure{
	
	final SQLStmt users = new SQLStmt("SELECT u.code, u.gender, u.age, u.country, u.signupDate FROM user u WHERE ? <= age AND age <= ?");
	
	public VoltTable run (int lowerBound, int upperBound){
		
		voltQueueSQL(users, lowerBound, upperBound);
		
		return voltExecuteSQL(true)[0];
		
	}
}
