package org.hivedb.meta.persistence;

import org.apache.commons.dbcp.BasicDataSource;

public class HiveBasicDataSource extends BasicDataSource {

	public HiveBasicDataSource(String hiveUri)
	{
		super();
		setUrl(hiveUri);		
	}
}
