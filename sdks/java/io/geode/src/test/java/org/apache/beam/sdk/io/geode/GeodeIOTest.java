package org.apache.beam.sdk.io.geode;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;

import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.junit.Rule;
import org.junit.Test;

/**
 * Created by sbawaskar on 7/13/17.
 */

public class GeodeIOTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void testWriteToGeode() {
    GfshScript.of("start locator --name=loc1",
        "start server --name=serv1",
        "create region --name=testRegion --type=PARTITION").execute(gfshRule);
    Map<Integer, String> input = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      input.put(i, "value" + i);
    }
//    Properties properties = new Properties(); // properties to run in local mode
//    properties.setProperty("locators", "");
//    properties.setProperty("mcast-port", "0");
    ClientCache cache = new ClientCacheFactory().setPoolReadTimeout(10000).create();
    Region r = cache.createClientRegionFactory(ClientRegionShortcut.LOCAL).create("testRegion");

    pipeline.apply(Create.of(input))
        .apply(GeodeIO.write().withLocators("localhost[10334]").toRegion("testRegion"));
    pipeline.run();
    assertEquals(100, r.sizeOnServer());
  }

//  @Rule
//  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();
//  private static String REGION_NAME = "AuthRegion";
//  private final VM client1 = startupRule.getVM(1);
//  private final VM client2 = startupRule.getVM(2);
//  private final VM client3 = startupRule.getVM(3);
//
//  @Rule
//  public ServerStarterRule server = new ServerStarterRule()
//      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName())
//      .withRegion(RegionShortcut.REPLICATE, REGION_NAME);
//  @Test public void testWriteToGeode() {
//    Map<Integer, String> input = new HashMap<>();
//    for (int i = 0; i < 100; i++) {
//      input.put(i, "value" + i);
//    }
//    Properties properties = new Properties(); // properties to run in local mode
//    properties.setProperty("locators", "");
//    properties.setProperty("mcast-port", "0");
//    ClientCache cache = new ClientCacheFactory(properties).setPoolReadTimeout(10000).create();
//    Region r = cache.createClientRegionFactory(ClientRegionShortcut.LOCAL).create("testRegion");
//
//    pipeline.apply(Create.of(input))
//        .apply(GeodeIO.write().withLocators("localhost[10334]").toRegion("testRegion"));
//    pipeline.run();
//    assertEquals(100, r.size());
//  }
}
