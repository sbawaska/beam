package org.apache.beam.sdk.io.geode;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.auto.value.AutoValue;
import com.google.common.net.HostAndPort;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * IO to write data into Apache Geode data grid.
 *
 * <p>GeodeIO sink supports writing key-value pairs to a Geode Region. To configure the Geode sink,
 * you must specify at the minimum Geode locators and the region to write to. For example:
 *
 * <pre>{@code
 *
 *  PCollection<KV<Long, String>> kvColl = ...;
 *  kvColl.apply(GeodeIO.<Long, String>write()
 *       .withLocators("hostname", port)
 *       .withRegion("results")
 *
 *    );
 * }</pre>
 */
@Experimental
public class GeodeIO {

  private static final Logger LOG = LoggerFactory.getLogger(GeodeIO.class);

  /**
   * write to the Geode cluster.
   */
  public static <K, V> Write write() {
    return new AutoValue_GeodeIO_Write.Builder<K, V>().build();
  }

  /**
   * A {@link PTransform} to write to a Geode Region. See {@link GeodeIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {

    @Nullable abstract String getLocators();
    @Nullable abstract String getRegionName();

    abstract Builder<K, V> builder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setLocators(String locators);
      abstract Builder<K, V> setRegionName(String name);
      abstract Write<K, V> build();
    }

    /**
     * Sets the locator hostname and port to use for discovering the Geode servers.
     * @param locators
     * @return
     */
    public Write<K, V> withLocators(String locators) {
      checkNotNull(locators);
      return builder().setLocators(locators).build();
    }

    /**
     * Sets the region in which the data is written.
     * @param name name of the region
     * @return
     */
    public Write<K, V> toRegion(String name) {
      checkNotNull(name);
      return builder().setRegionName(name).build();
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
      input.apply(ParDo.of(new GeodeWriter(this)));
      return PDone.in(input.getPipeline());
    }

  }

  private static class GeodeWriter<K, V> extends DoFn<KV<K, V>, Void> {

    private String locators;
    private String regionName;
    private ClientCache clientCache;
    private Region<K, V> region;
    private Map<K, V> putAllBuffer = new HashMap<>();

    public GeodeWriter(Write<K, V> kvWrite) {
      super();
      this.locators = kvWrite.getName();
      this.regionName = kvWrite.getRegionName();
    }

    @Setup
    public void setup() {
      this.clientCache = createClientCache();
      this.region = createRegion();
    }

    private ClientCache createClientCache() {
      if (ClientCacheFactory.getAnyInstance() != null) {
        return ClientCacheFactory.getAnyInstance();
      }
      HostAndPort address = HostAndPort.fromString(locators);
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.addPoolLocator(address.getHost(), address.getPort());
      return clientCacheFactory.create();
    }

    private Region<K, V> createRegion() {
      if (this.clientCache.getRegion(this.regionName) != null) {
        return this.clientCache.getRegion(this.regionName);
      }
      ClientRegionFactory<K, V> clientRegionFactory = clientCache.
          createClientRegionFactory(ClientRegionShortcut.PROXY);
      return clientRegionFactory.create(regionName);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      putAllBuffer.put(context.element().getKey(), context.element().getValue());
    }

    @FinishBundle
    public void finishBundle() {
      this.region.putAll(putAllBuffer);
      this.putAllBuffer.clear();
    }

    @Teardown
    public void tearDown() {
      this.clientCache.close();
    }
  }
}
