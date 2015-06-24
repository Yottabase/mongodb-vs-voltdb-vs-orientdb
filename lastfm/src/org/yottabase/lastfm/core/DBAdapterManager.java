package org.yottabase.lastfm.core;

import java.util.ArrayList;
import java.util.List;

public class DBAdapterManager {
	
	private PropertyFile properties;
	
	public DBAdapterManager(PropertyFile properties) {
		super();
		this.properties = properties;
	}

	public PropertyFile getProperties() {
		return properties;
	}

	public void setProperties(PropertyFile properties) {
		this.properties = properties;
	}

	public List<AbstractDBFacade> getAdapters(){
		
		String[] dbAdapterNames = properties.get("supported_adapters").split(",");

		List<AbstractDBFacade> adapters = new ArrayList<AbstractDBFacade>();

		for (String adapterName : dbAdapterNames) {
			String enabledFlagKey = adapterName + ".enabled";
		
			if (properties.get(enabledFlagKey).equals("true")) {
				adapters.add(this.getAdapter(adapterName));
			}
		}

		return adapters;
	}
	
	public AbstractDBFacade getAdapter(String adapterName){
		AbstractDBFacade adapter = null;
		
		
		String adapterFactoryKey = adapterName + ".factory";

		String factoryClassName = properties.get(adapterFactoryKey);

		try {
			DBFacadeFactory adapterFactory = (DBFacadeFactory) Class
					.forName(factoryClassName).newInstance();
			adapter = adapterFactory.createService(properties);

		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return adapter;
	}

}
