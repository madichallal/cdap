/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.writer.MetadataOperation;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.scheduler.Scheduler;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deleted program handler stage. Figures out which programs are deleted and handles callback.
 */
public class DeletedProgramHandlerStage extends AbstractStage<ApplicationDeployable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeletedProgramHandlerStage.class);

  private final Store store;
  private final ProgramTerminator programTerminator;
  private final StreamConsumerFactory streamConsumerFactory;
  private final QueueAdmin queueAdmin;
  private final MetricStore metricStore;
  private final MetadataPublisher metadataPublisher;
  private final Impersonator impersonator;
  private final Scheduler programScheduler;

  public DeletedProgramHandlerStage(Store store, ProgramTerminator programTerminator,
                                    StreamConsumerFactory streamConsumerFactory,
                                    QueueAdmin queueAdmin, MetricStore metricStore,
                                    MetadataPublisher metadataPublisher, Impersonator impersonator,
                                    Scheduler programScheduler) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.store = store;
    this.programTerminator = programTerminator;
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.metricStore = metricStore;
    this.metadataPublisher = metadataPublisher;
    this.impersonator = impersonator;
    this.programScheduler = programScheduler;
  }

  @Override
  public void process(ApplicationDeployable appSpec) throws Exception {
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appSpec.getApplicationId(),
                                                                                    appSpec.getSpecification());

    // TODO: this should also delete logs and run records (or not?), and do it for all program types [CDAP-2187]

    List<String> deletedFlows = Lists.newArrayList();
    for (ProgramSpecification spec : deletedSpecs) {
      //call the deleted spec
      ProgramType type = ProgramTypes.fromSpecification(spec);
      final ProgramId programId = appSpec.getApplicationId().program(type, spec.getName());
      programTerminator.stop(programId);
      programScheduler.deleteSchedules(programId);
      programScheduler.modifySchedulesTriggeredByDeletedProgram(programId);

      // drop all queues and stream states of a deleted flow
      if (ProgramType.FLOW.equals(type)) {
        FlowUtils.clearDeletedFlow(impersonator, queueAdmin, streamConsumerFactory, programId,
                                   (FlowSpecification) spec);
        deletedFlows.add(programId.getEntityName());
      }

      // Remove metadata for the deleted program
      metadataPublisher.publish(NamespaceId.SYSTEM, new MetadataOperation.Drop(programId.toMetadataEntity()));
    }
    if (!deletedFlows.isEmpty()) {
      deleteMetrics(appSpec.getApplicationId(), deletedFlows);
    }

    emit(appSpec);
  }

  private void deleteMetrics(ApplicationId applicationId, Iterable<String> flows) {
    LOG.debug("Deleting metrics for application {}", applicationId);
    for (String flow : flows) {
      long endTs = System.currentTimeMillis() / 1000;
      Map<String, String> tags = new LinkedHashMap<>();
      tags.put(Constants.Metrics.Tag.NAMESPACE, applicationId.getNamespace());
      tags.put(Constants.Metrics.Tag.APP, applicationId.getApplication());
      tags.put(Constants.Metrics.Tag.FLOW, flow);
      MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, Collections.emptySet(), tags,
                                                            new ArrayList<>(tags.keySet()));
      metricStore.delete(deleteQuery);
    }
  }
}
