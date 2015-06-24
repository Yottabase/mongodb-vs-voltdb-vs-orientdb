package org.yottabase.lastfm.driver.noop;

import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class NoopFacadeFactory implements FacadeFactory {

	@Override
	public Facade createService(PropertyFile properties) {
		
		return new NoopFacade();
	}

}
