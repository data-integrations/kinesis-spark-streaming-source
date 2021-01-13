/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.format.RecordFormats;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Util class for KinesisStreamingSource.
 */
final class KinesisStreamingSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamingSourceUtil.class);

  /**
   * Returns JavaDstream.
   */
  static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(
    StreamingContext streamingContext, KinesisStreamingSource.KinesisStreamConfig config) {
    BasicAWSCredentials awsCred = new BasicAWSCredentials(config.getAwsAccessKeyId(), config.getAwsAccessSecret());
    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(awsCred);
    kinesisClient.setEndpoint(config.getEndpoint());
    JavaStreamingContext javaStreamingContext = streamingContext.getSparkStreamingContext();
    Duration kinesisCheckpointInterval = new Duration(config.getDuration());

    int numShards = kinesisClient.describeStream(config.getStreamName()).getStreamDescription().getShards().size();
    List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numShards);
    LOG.debug("creating {} spark executors for {} shards", numShards, numShards);
    //Creating spark executors based on the number of shards in the stream
    for (int i = 0; i < numShards; i++) {
      streamsList.add(
        KinesisUtils.createStream(javaStreamingContext, config.getAppName(), config.getStreamName(),
                                  config.getEndpoint(), config.getRegion(), config.getInitialPosition(),
                                  kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2(),
                                  config.getAwsAccessKeyId(), config.getAwsAccessSecret())
      );
    }

    // Union all the streams if there is more than 1 stream
    JavaDStream<byte[]> kinesisStream;
    if (streamsList.size() > 1) {
      kinesisStream = javaStreamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
    } else {
      // Otherwise, just use the 1 stream
      kinesisStream = streamsList.get(0);
    }
    return kinesisStream.map(config.getFormat() == null ? new BytesFunction(config) : new FormatFunction(config));
  }

  /**
   * Transforms Kinesis payload into a structured record when message format is not given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   * Output schema must contain only 1 field of type bytes
   */
  private static class BytesFunction implements Function<byte[], StructuredRecord> {
    private final KinesisStreamingSource.KinesisStreamConfig config;
    private transient String messageField;
    private transient Schema schema;

    BytesFunction(KinesisStreamingSource.KinesisStreamConfig config) {
      this.config = config;
    }

    @Override
    public StructuredRecord call(byte[] data) throws Exception {
      // first time this was called, initialize schema
      if (schema == null) {
        schema = config.parseSchema();
        StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
        messageField = schema.getFields().get(0).getName();
        recordBuilder.set(messageField, data);
        return recordBuilder.build();
      }
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      recordBuilder.set(messageField, data);
      return recordBuilder.build();
    }
  }

  /**
   * Transforms Kinesis payload into a structured record when message format and schema are given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class FormatFunction implements Function<byte[], StructuredRecord> {
    private final KinesisStreamingSource.KinesisStreamConfig config;
    private transient Schema outputSchema;
    private transient RecordFormat<ByteBuffer, StructuredRecord> recordFormat;

    FormatFunction(KinesisStreamingSource.KinesisStreamConfig config) {
      this.config = config;
    }

    @Override
    public StructuredRecord call(byte[] data) throws Exception {
      // first time this was called, initialize schema
      if (recordFormat == null) {
        outputSchema = config.parseSchema();
        Schema messageSchema = config.parseSchema();
        FormatSpecification spec =
          new FormatSpecification(config.getFormat(), messageSchema, new HashMap<String, String>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      StructuredRecord messageRecord = recordFormat.read(ByteBuffer.wrap(data));
      for (Schema.Field messageField : messageRecord.getSchema().getFields()) {
        String fieldName = messageField.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
      return builder.build();
    }
  }

  private KinesisStreamingSourceUtil() {
    // no-op
  }
}
