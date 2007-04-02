package org.hivedb.meta.command;

import org.hivedb.Hive;

/**
 * @author Justin McCarthy (jmccarty@cafepress.com)
 */
public abstract class HiveCommand {
	protected String hiveUri = null;
	// command controller (the Tools class) will inject the URI of the global hive instance
	public void setJdbcUri(String hiveUri)
	{
		this.hiveUri = hiveUri;
	}
	protected String getJdbcUri()
	{
		return this.hiveUri;
	}
	public abstract void execute(Hive hive);
}