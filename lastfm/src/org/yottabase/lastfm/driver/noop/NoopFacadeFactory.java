package org.yottabase.lastfm.driver.noop;

import java.util.Properties;

import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeFactory;

public class NoopFacadeFactory implements FacadeFactory {

	@Override
	public Facade createService(Properties properties) {
		
		return new NoopFacade();
	}

}
