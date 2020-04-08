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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Spark streaming source to get data from AWS Kinesis streams
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("KinesisSource")
@Description("Kinesis streaming source.")
public class KinesisStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private final KinesisStreamConfig config;

  public KinesisStreamingSource(KinesisStreamConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    super.prepareRun(context);

    Schema schema = context.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(context, config.referenceName, schema,
                    "Read", String.format("Read from Kinesis Stream named %s.", config.streamName));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    registerUsage(streamingContext);
    return KinesisStreamingSourceUtil.getStructuredRecordJavaDStream(streamingContext, config);
  }

  /**
   * config file for Kinesis stream sink
   */
  public static class KinesisStreamConfig extends ReferencePluginConfig implements Serializable {
    private static final String APP_NAME = "appName";
    private static final String STREAM_NAME = "streamName";
    private static final String ENDPOINT_URL = "endpointUrl";
    private static final String REGION = "region";
    private static final String ACCESS_KEY_ID = "awsAccessKeyId";
    private static final String ACCESS_SECRET = "awsAccessSecret";
    private static final String SCHEMA = "schema";

    @Name(APP_NAME)
    @Description("The application name that will be used to checkpoint the Kinesis sequence numbers in DynamoDB table")
    @Macro
    private final String appName;

    @Name(STREAM_NAME)
    @Description("The name of the Kinesis stream to the get the data from. The stream should be active")
    @Macro
    private final String streamName;

    @Name(ENDPOINT_URL)
    @Description("Valid Kinesis endpoint URL eg. kinesis.us-east-1.amazonaws.com")
    @Macro
    private final String endpoint;

    @Name(REGION)
    @Description("AWS region specific to the stream")
    @Macro
    private final String region;

    @Name("duration")
    @Description("The interval in milliseconds (e.g., Duration(2000) = 2 seconds) at which the Kinesis Client " +
      "Library saves its position in the stream.")
    @Macro
    private final Integer duration;

    @Name("initialPosition")
    @Description("Can be either TRIM_HORIZON or LATEST, Default position will be Latest")
    private final String initialPosition;

    @Name(ACCESS_KEY_ID)
    @Description("AWS access Id having access to Kinesis streams")
    @Macro
    private final String awsAccessKeyId;

    @Name(ACCESS_SECRET)
    @Description("AWS access key secret having access to Kinesis streams")
    @Macro
    private final String awsAccessSecret;

    @Description("Optional format of the Kinesis stream shard. Any format supported by CDAP is supported. For " +
      "example, a value of 'csv' will attempt to parse Kinesis payloads as comma-separated values. If no format is " +
      "given, Kinesis shard payloads will be treated as bytes.")
    @Nullable
    private final String format;

    @Description("Output schema of the source, The fields are used in conjunction with the format to parse Kinesis " +
      "payloads")
    private final String schema;

    public KinesisStreamConfig(String referenceName, String appName, String streamName, String endpoint, String region,
                               Integer duration, String initialPosition, String awsAccessKeyId, String awsAccessSecret,
                               String format, String schema) {
      super(referenceName);
      this.appName = appName;
      this.streamName = streamName;
      this.endpoint = endpoint;
      this.region = region;
      this.duration = duration;
      this.initialPosition = initialPosition;
      this.awsAccessKeyId = awsAccessKeyId;
      this.awsAccessSecret = awsAccessSecret;
      this.format = format;
      this.schema = schema;
    }

    @Nullable
    public String getFormat() {
      return Strings.isNullOrEmpty(format) ? null : format;
    }

    public Schema parseSchema() {
      try {
        return Schema.parseJson(schema);
      } catch (IOException | NullPointerException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }

    public void validate(FailureCollector collector) {
      if (!containsMacro(STREAM_NAME) && Strings.isNullOrEmpty(streamName)) {
        collector.addFailure("Stream name should be non-null, non-empty.", null).withConfigProperty(STREAM_NAME);
      }
      if (!containsMacro(ACCESS_KEY_ID) && Strings.isNullOrEmpty(awsAccessKeyId)) {
        collector.addFailure("Access Key should be non-null, non-empty.", null).withConfigProperty(ACCESS_KEY_ID);
      }
      if (!containsMacro(ACCESS_SECRET) && Strings.isNullOrEmpty(awsAccessSecret)) {
        collector.addFailure("Access Key secret should be non-null, non-empty.", null)
          .withConfigProperty(ACCESS_SECRET);
      }
      if (!containsMacro(REGION) && Strings.isNullOrEmpty(region)) {
        collector.addFailure("Region name should be non-null, non-empty.", null).withConfigProperty(REGION);
      }
      if (!containsMacro(APP_NAME) && Strings.isNullOrEmpty(appName)) {
        collector.addFailure("Application name should be non-null, non-empty.", null).withConfigProperty(APP_NAME);
      }
      if (!containsMacro(ENDPOINT_URL) && Strings.isNullOrEmpty(endpoint)) {
        collector.addFailure("Endpoint url should be non-null, non-empty.", null).withConfigProperty(ENDPOINT_URL);
      }
      if (!containsMacro(SCHEMA) && Strings.isNullOrEmpty(schema)) {
        collector.addFailure("Schema should be non-null, non-empty.", null).withConfigProperty(SCHEMA);
      }

      if (!containsMacro(SCHEMA) && !Strings.isNullOrEmpty(schema)) {
        try {
          Schema messageSchema = parseSchema();
          // if format is empty, there must be just a single message field of type bytes or nullable types.
          if (Strings.isNullOrEmpty(format)) {
            List<Schema.Field> messageFields = messageSchema.getFields();
            if (messageFields.size() > 1) {
              for (int i = 1; i < messageFields.size(); i++) {
                Schema.Field messageField = messageFields.get(i);
                collector.addFailure(
                  String.format("Without format, field '%s' must contain just a single message field of type " +
                                  "bytes or nullable bytes.", messageField.getName()),
                  "Remove the field or change the format.")
                  .withOutputSchemaField(messageField.getName());
              }
            }
            Schema.Field messageField = messageFields.get(0);
            Schema messageFieldSchema = messageField.getSchema().isNullable() ?
              messageField.getSchema().getNonNullable() : messageField.getSchema();
            Schema.Type messageFieldType = messageFieldSchema.getType();
            Schema.LogicalType messageFieldLogicalType = messageFieldSchema.getLogicalType();
            if (messageFieldType != Schema.Type.BYTES || messageFieldLogicalType != null) {
              collector.addFailure(
                String.format(
                  "Without a format, the message field must be of type bytes or nullable bytes, " +
                    "but field '%s' is of unexpected type '%s'.",
                  messageField.getName(), messageFieldSchema.getDisplayName()), null)
                .withOutputSchemaField(messageField.getName());
            }
          } else {
            // otherwise, if there is a format, make sure we can instantiate it.
            FormatSpecification formatSpec = new FormatSpecification(format, messageSchema, new HashMap<>());

            try {
              RecordFormats.createInitializedFormat(formatSpec);
            } catch (Exception e) {
              collector.addFailure(String.format(
                "Unable to instantiate a message parser from format '%s': %s", format, e.getMessage()), null)
                .withStacktrace(e.getStackTrace());
            }
          }
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(SCHEMA).withStacktrace(e.getStackTrace());
        }
      }
    }

    InitialPositionInStream getInitialPosition() {
      if (initialPosition.equalsIgnoreCase(InitialPositionInStream.TRIM_HORIZON.name())) {
        return InitialPositionInStream.TRIM_HORIZON;
      } else {
        return InitialPositionInStream.LATEST;
      }
    }

    String getAppName() {
      return appName;
    }

    String getStreamName() {
      return streamName;
    }

    String getEndpoint() {
      return endpoint;
    }

    String getRegion() {
      return region;
    }

    Integer getDuration() {
      return duration;
    }

    String getAwsAccessKeyId() {
      return awsAccessKeyId;
    }

    String getAwsAccessSecret() {
      return awsAccessSecret;
    }

    String getSchema() {
      return schema;
    }
  }
}
