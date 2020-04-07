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

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.tephra.TransactionFailureException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Base streaming source that adds an External Dataset for a reference name, and performs a single getDataset()
 * call to make sure CDAP records that it was accessed.
 *
 * @param <T> type of object read by the source.
 */
public abstract class ReferenceStreamingSource<T> extends StreamingSource<T> {

  private final ReferencePluginConfig conf;

  public ReferenceStreamingSource(ReferencePluginConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // Verify that reference name meets dataset id constraints
    IdUtils.validateReferenceName(conf.referenceName, pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.createDataset(conf.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
  }

  protected void registerUsage(Transactional transactional) throws TransactionFailureException {
    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) {
        datasetContext.getDataset(conf.referenceName);
      }
    });
  }

  /**
   * Record field-level lineage for streaming source plugins (ReadOperation). This method should be called from
   * prepareRun of any streaming source plugin.
   *
   * @param context StreamingSourceContext from prepareRun
   * @param outputName name of output dataset
   * @param tableSchema schema of fields. Also used to determine list of field names. Schema and schema.getFields() must
   * not be null.
   * @param operationName name of the operation
   * @param description operation description; complete sentences preferred
   */
  protected void recordLineage(StreamingSourceContext context, String outputName, Schema tableSchema,
                               String operationName, String description)
    throws DatasetManagementException, TransactionFailureException {
    Preconditions.checkNotNull(tableSchema, "schema for output %s is null.", outputName);
    Preconditions.checkNotNull(tableSchema.getFields(), "schema.getFields() for output %s is null.", outputName);

    context.registerLineage(outputName, tableSchema);
    List<String> fieldNames = tableSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    if (!fieldNames.isEmpty()) {
      LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
      lineageRecorder.recordRead(operationName, description, fieldNames);
    }
  }
}
