package org.yottabase.lastfm.driver.noop;

import org.yottabase.lastfm.core.DBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class NoopFacadeFactory implements DBFacadeFactory {

	@Override
	public DBFacade createService(PropertyFile properties) {
		
		return new NoopFacade();
	}

}
