package org.yottabase.lastfm.adapter.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class AverageNumberSungTracksPerArtist extends VoltProcedure{

	final SQLStmt avg = new SQLStmt("SELECT AVG(atg.num) FROM (SELECT count(a.code) as num FROM artist a LEFT JOIN track t ON (a.code = t.artistcode) GROUP BY a.code) atg;");
	
	public long run (){
		
		voltQueueSQL(avg);
		
		return voltExecuteSQL(true)[0].asScalarLong();
		
	}
}
