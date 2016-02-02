package com.sqrrl.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.sqrrl.Master;
import com.sqrrl.Tablet;
import com.sqrrl.TabletServer;

/**
 * This class implements basic functionality for Tablet Servers.
 * 
 * @author gmanoli
 *
 */
public class MasterImpl extends Master {

	/**
	 * Range value for each tablet.
	 */
	private long chunkSize = Long.MAX_VALUE / numTablets;

	/**
	 * The list of created tablets.
	 */
	private List<Tablet> tablets;

	/**
	 * The tablet servers mapping.
	 */
	private Map<String, TabletServer> tabletServers;

	public MasterImpl(int numTablets, List<String> serverNames) {
		super(numTablets, serverNames);
		initTablets(numTablets);
		initTabletServers(serverNames);
		assignTabletsToServers();
	}

	/**
	 * Assign tablets to servers.
	 */
	private void assignTabletsToServers() {
		for (int i = 0; i < tablets.size(); i++) {
			String serverName = serverNames.get(i % serverNames.size());
			tabletServers.get(serverName).addTablet(tablets.get(i));
		}
	}

	/**
	 * Creates the tablet servers.
	 * 
	 * @param serverNames
	 *            The list of server names.
	 */
	private void initTabletServers(List<String> serverNames) {
		tabletServers = new HashMap<String, TabletServer>();

		for (String serverName : serverNames) {
			tabletServers.put(serverName, new TabletServer());
		}
	}

	/**
	 * Creates the list of tablets.
	 * 
	 * @param numTablets
	 *            The amount of tablets to create.
	 */
	private void initTablets(int numTablets) {
		tablets = new ArrayList<Tablet>(numTablets);
		long minRangeValue = 0;
		for (int i = 0; i < numTablets; i++) {
			tablets.add(new Tablet(minRangeValue, minRangeValue + chunkSize));
			minRangeValue += chunkSize;
		}
	}

	@Override
	public String getServerForKey(long key) {

		for (Entry<String, TabletServer> entry : tabletServers.entrySet()) {
			String serverName = entry.getKey();
			TabletServer tabletServer = entry.getValue();
			if (tabletServer.containsKey(key)) {
				return serverName;
			}
		}
		return null;
	}

	@Override
	public void addServer(String serverName) {
		if (!serverNames.contains(serverName)) {
			serverNames.add(serverName);
			reallocateTablets(serverName);
		}
	}

	/**
	 * Re allocates tablets among the tablet servers when a new tablet server is
	 * created.
	 * 
	 * @param serverName
	 *            The server name to be created.
	 */
	private void reallocateTablets(String serverName) {
		TabletServer newTableServer = new TabletServer();
		tabletServers.put(serverName, newTableServer);

		while (!tabletServersBalanced()) {
			TabletServer maxTabletServerLoad = getMaxTabletServerLoad();
			Tablet lastTablet = maxTabletServerLoad.getLastTablet();
			newTableServer.addTablet(lastTablet);
		}

	}

	/**
	 * Returns the tablet server with the max amount of tablets assigned.
	 * 
	 * @return The tablet server with the max amount of tablets.
	 */
	private TabletServer getMaxTabletServerLoad() {
		TabletServer maxTabletServer = null;

		for (TabletServer tabletServer : tabletServers.values()) {
			if (maxTabletServer == null
					|| tabletServer.getTabletsSize() > maxTabletServer
							.getTabletsSize()) {
				maxTabletServer = tabletServer;
			}
		}
		return maxTabletServer;
	}

	/**
	 * Checks if the tablet servers are balanced.
	 * 
	 * @return true if all the tablet servers are balanced.
	 */
	private boolean tabletServersBalanced() {
		int balancedTablets = numTablets / serverNames.size();

		for (TabletServer tabletServer : tabletServers.values()) {

			if (tabletServer.isNotBalanced(balancedTablets)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void removeServer(String serverName) {
		if (serverNames.contains(serverName)) {
			serverNames.remove(serverName);
			TabletServer tabletServer = tabletServers.remove(serverName);
			allocateTablets(tabletServer.getTablets());
		}
	}

	/**
	 * Allocates a list a tablets in the available tablet servers.
	 * 
	 * @param tablets
	 *            The list of tablets to be allocated.
	 */
	private void allocateTablets(List<Tablet> tablets) {
		for (Tablet tablet : tablets) {
			TabletServer tabletServer = getAvailableTabletServer();
			tabletServer.addTablet(tablet);
		}
	}

	/**
	 * Returns a tablet server which has available space to assign a new tablet.
	 * 
	 * @return The tablet server to allocate a new tablet.
	 */
	private TabletServer getAvailableTabletServer() {

		TabletServer availableTabletServer = getMaxTabletServerLoad();
		for (TabletServer tabletServer : tabletServers.values()) {

			if (tabletServer.getTabletsSize() < availableTabletServer
					.getTabletsSize()) {
				availableTabletServer = tabletServer;
			}

		}
		return availableTabletServer;
	}

}
