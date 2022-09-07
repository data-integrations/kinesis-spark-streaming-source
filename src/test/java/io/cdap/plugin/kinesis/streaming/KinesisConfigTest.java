/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.kinesis.streaming;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for KinesisStreamConfig
 */
public class KinesisConfigTest {

  // test with null format, output schema with only 1 field of type bytes expected
  @Test
  public void testConfigurations() {
    Schema schema = Schema.recordOf("kinesis", Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
    String endpointUrl = "kinesis.us-east-1.amazonaws.com";
    String region = "us-east-1";
    KinesisStreamingSource.KinesisStreamConfig config;
    config = new KinesisStreamingSource.KinesisStreamConfig("KinesisSource", "newapp", "teststream", endpointUrl,
                                                            region, 2000, "TRIM_HORIZON", "someKey", "someId", null,
                                                            schema.toString());

    Schema expected = Schema.recordOf("kinesis", Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
    Schema actual = config.parseSchema();
    Assert.assertEquals(expected, actual);

    // test with csv format
    schema = Schema.recordOf(
      "kinesis",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );
    config = new KinesisStreamingSource.KinesisStreamConfig("KinesisSource", "newapp", "teststream", endpointUrl,
                                                            region, 2000, "TRIM_HORIZON", "someKey", "someId", "csv",
                                                            schema.toString());
    expected = Schema.recordOf(
      "kinesis",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );
    actual = config.parseSchema();
    Assert.assertEquals(expected, actual);
  }

  // Test with null schema
  @Test
  public void testNullSchemaError() {
    String endpointUrl = "kinesis.us-east-1.amazonaws.com";
    String region = "us-east-1";
    KinesisStreamingSource.KinesisStreamConfig config;
    config = new KinesisStreamingSource.KinesisStreamConfig("KinesisSource", "newapp", "teststream", endpointUrl,
                                                            region, 2000, "TRIM_HORIZON", "someKey", "someId", null,
                                                            null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
    Assert.assertEquals("schema", failures.get(0).getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  // Test with null format and output schema with field of type string
  @Test
  public void testNullFormatStringOutputError() {
    Schema schema = Schema.recordOf("kinesis", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    String endpointUrl = "kinesis.us-east-1.amazonaws.com";
    String region = "us-east-1";
    KinesisStreamingSource.KinesisStreamConfig config;
    config = new KinesisStreamingSource.KinesisStreamConfig("KinesisSource", "newapp", "teststream", endpointUrl,
                                                            region, 2000, "TRIM_HORIZON", "someKey", "someId", null,
                                                            schema.toString());
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
    Assert.assertEquals("body", failures.get(0).getCauses().get(0).getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
  }

  // Test with null format and multiple fields in output schema
  @Test
  public void testMultipleFieldsError() {
    Schema schema = Schema.recordOf("kinesis", Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("body2", Schema.of(Schema.Type.STRING)));
    String endpointUrl = "kinesis.us-east-1.amazonaws.com";
    String region = "us-east-1";
    KinesisStreamingSource.KinesisStreamConfig config;
    config = new KinesisStreamingSource.KinesisStreamConfig("KinesisSource", "newapp", "teststream", endpointUrl,
                                                            region, 2000, "TRIM_HORIZON", "someKey", "someId", null,
                                                            schema.toString());
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertEquals("body2", failures.get(0).getCauses().get(0).getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
    Assert.assertEquals("body", failures.get(1).getCauses().get(0).getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
  }

  @Test
  public void testBadFormatErrors() {
    Schema schema = Schema.recordOf("kinesis", Schema.Field.of("body", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("body2", Schema.of(Schema.Type.STRING)));
    String endpointUrl = "kinesis.us-east-1.amazonaws.com";
    String region = "us-east-1";
    KinesisStreamingSource.KinesisStreamConfig config;
    config = new KinesisStreamingSource.KinesisStreamConfig("KinesisSource", "newapp", "teststream", endpointUrl,
                                                            region, 2000, "TRIM_HORIZON", "someKey", "someId",
                                                            "badFormat", schema.toString());
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
  }
}
