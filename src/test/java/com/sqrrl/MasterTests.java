package com.sqrrl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sqrrl.impl.MasterImpl;

public class MasterTests {

	private List<String> serverNames;
	private Master master;

	@Before
	public void setup() {
		serverNames = new ArrayList<>();
		serverNames.add("tabletserver0");
		serverNames.add("tabletserver1");
		master = new MasterImpl(4, serverNames);
	}

	@Test(expected = IllegalStateException.class)
	public void testInitMasterZeroTablets() {
		master = new MasterImpl(0, serverNames);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInitMasterEmptyServers() {
		master = new MasterImpl(4, new ArrayList<String>());
	}

	@Test
	public void testInitTableServers() {
		Assert.assertEquals("tabletserver0", master.getServerForKey(0L));
		Assert.assertEquals("tabletserver1",
				master.getServerForKey(2305843009213693951L));
		Assert.assertEquals("tabletserver0",
				master.getServerForKey(4611686018427387902L));
		Assert.assertEquals("tabletserver1",
				master.getServerForKey(6917529027641081853L));

	}

	@Test
	public void testAddServer() {
		master.addServer("tabletserver2");
		Assert.assertEquals("tabletserver0", master.getServerForKey(0L));
		Assert.assertEquals("tabletserver1",
				master.getServerForKey(2305843009213693951L));
		Assert.assertEquals("tabletserver0",
				master.getServerForKey(4611686018427387902L));
		Assert.assertEquals("tabletserver2",
				master.getServerForKey(6917529027641081853L));
	}

	@Test
	public void testRemoveServer() {
		master.removeServer("tabletserver0");
		Assert.assertEquals("tabletserver1", master.getServerForKey(0L));
		Assert.assertEquals("tabletserver1",
				master.getServerForKey(2305843009213693951L));
		Assert.assertEquals("tabletserver1",
				master.getServerForKey(4611686018427387902L));
		Assert.assertEquals("tabletserver1",
				master.getServerForKey(6917529027641081853L));
	}

	@Test
	public void testAddAndRemoveServer() {
		master.addServer("tabletserver2");
		master.removeServer("tabletserver0");
		Assert.assertEquals("tabletserver1", master.getServerForKey(0L));
		Assert.assertEquals("tabletserver1",
				master.getServerForKey(2305843009213693951L));
		Assert.assertEquals("tabletserver2",
				master.getServerForKey(4611686018427387902L));
		Assert.assertEquals("tabletserver2",
				master.getServerForKey(6917529027641081853L));
	}

}
