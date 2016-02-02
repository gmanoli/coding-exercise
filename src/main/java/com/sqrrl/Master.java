package com.sqrrl;

import java.util.List;

import org.apache.commons.lang3.Validate;

public abstract class Master {

	protected int numTablets;
	protected List<String> serverNames;

	public Master(int numTablets, List<String> serverNames) {
		Validate.validState(numTablets > 0);
		Validate.notEmpty(serverNames);
		this.numTablets = numTablets;
		this.serverNames = serverNames;
	}

	/**
	 * Returns the server name given a key.
	 * 
	 * @param key
	 *            The key value belonging to a range.
	 * @return The server name.
	 */
	public abstract String getServerForKey(long key);

	/**
	 * Adds a new server and balances the load among them.
	 * 
	 * @param serverName
	 *            The server name.
	 */
	public abstract void addServer(String serverName);

	/**
	 * Removes an existent server.
	 * 
	 * @param serverName
	 *            The server name.
	 */
	public abstract void removeServer(String serverName);
}