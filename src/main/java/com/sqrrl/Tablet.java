package com.sqrrl;

/**
 * This class represents a range of keys to different servers.
 * 
 * @author gmanoli
 *
 */
public class Tablet {

	private long minRangeValue;
	private long maxRangeValue;

	public Tablet(long minRangeValue, long maxRangeValue) {
		this.minRangeValue = minRangeValue;
		this.maxRangeValue = maxRangeValue;
	}

	/**
	 * Verifies if a key belongs to this tablet.
	 * 
	 * @param key
	 *            The value to be verified.
	 * @return
	 */
	public boolean containsKey(long key) {

		return (minRangeValue <= key && key < maxRangeValue);
	}

	@Override
	public String toString() {
		return "Tablet [minRangeValue=" + minRangeValue + ", maxRangeValue="
				+ maxRangeValue + "]";
	}

}
