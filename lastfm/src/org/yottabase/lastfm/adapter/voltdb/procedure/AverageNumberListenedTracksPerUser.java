package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class AverageNumberListenedTracksPerUser extends VoltProcedure{

	final SQLStmt avg = new SQLStmt("SELECT AVG(ug.num) FROM (SELECT COUNT(u.code) as num FROM user u LEFT JOIN listenedtrack l ON (u.code = l.usercode) GROUP BY u.code) ug");
	
	public long run (){
		
		voltQueueSQL(avg);
		
		return voltExecuteSQL(true)[0].asScalarLong();
		
	}
}
