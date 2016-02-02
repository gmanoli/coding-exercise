package com.sqrrl;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a tablet server which serves tablets.
 * 
 * @author gmanoli
 *
 */
public class TabletServer {

	private List<Tablet> tablets;

	public TabletServer() {
		tablets = new ArrayList<>();
	}

	/**
	 * Returns the number of tablet in the table server.
	 * 
	 * @return The number of tablet in the table server.
	 */
	public int getTabletsSize() {
		return tablets.size();
	}

	/**
	 * Adds a tablet.
	 * 
	 * @param tablet
	 *            The table to be added.
	 */
	public void addTablet(Tablet tablet) {
		tablets.add(tablet);
	}

	/**
	 * Removes and returns the last tablet in the list.
	 * 
	 * @return The last tablet.
	 */
	public Tablet getLastTablet() {
		return tablets.remove(tablets.size() - 1);
	}

	/**
	 * Verifies if a key belongs to the list of tablets.
	 * 
	 * @param key
	 *            The value to be verified.
	 * @return true if belongs to the list.
	 */
	public boolean containsKey(long key) {
		for (Tablet tablet : tablets) {
			if (tablet.containsKey(key)) {
				return true;
			}
		}
		return false;
	}

	public List<Tablet> getTablets() {
		return tablets;
	}

	/**
	 * Controls if the tablet server is not balanced.
	 * 
	 * @param balancedTablets
	 *            The value took as a min and max load limit.
	 * @return true if is not balanced, false otherwise.
	 */
	public boolean isNotBalanced(int balancedTablets) {
		return (tablets.size() > balancedTablets + 1 || tablets.size() < balancedTablets);
	}

	@Override
	public String toString() {
		return "TabletServer [tablets=" + tablets + "]";
	}

}
