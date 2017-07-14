package org.apache.beam.sdk.io.geode;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sbawaskar on 7/13/17.
 */

class GeodeIOTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testWriteToGeode() {
        Map<Integer, String> input = new HashMap<>();
        for (int i=0; i<100; i++) {
            input.put(i, "value"+i);
        }
        pipeline.apply(Create.of(input))
                .apply(GeodeIO.write().withLocators("localhost[10334]").toRegion("foo"));
        pipeline.run();
    }
}