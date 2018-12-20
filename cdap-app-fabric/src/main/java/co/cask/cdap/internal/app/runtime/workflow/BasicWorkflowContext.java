/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.metadata.MetadataReader;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.workflow.ConditionSpecification;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.messaging.MessagingService;
import com.google.common.collect.ImmutableMap;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of a {@link WorkflowContext}.
 */
final class BasicWorkflowContext extends AbstractContext implements WorkflowContext {

  private final WorkflowSpecification workflowSpec;
  private final ConditionSpecification conditionSpecification;
  private final WorkflowToken token;
  private final Map<String, WorkflowNodeState> nodeStates;
  private ProgramState state;
  private boolean consolidateFieldOperations;

  BasicWorkflowContext(WorkflowSpecification workflowSpec,
                       WorkflowToken token, Program program, ProgramOptions programOptions, CConfiguration cConf,
                       MetricsCollectionService metricsCollectionService,
                       DatasetFramework datasetFramework, TransactionSystemClient txClient,
                       DiscoveryServiceClient discoveryServiceClient, Map<String, WorkflowNodeState> nodeStates,
                       @Nullable PluginInstantiator pluginInstantiator,
                       SecureStore secureStore, SecureStoreManager secureStoreManager,
                       MessagingService messagingService, @Nullable ConditionSpecification conditionSpecification,
                       MetadataReader metadataReader, MetadataPublisher metadataPublisher,
                       NamespaceQueryAdmin namespaceQueryAdmin) {
    super(program, programOptions, cConf, new HashSet<>(),
          datasetFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, Collections.singletonMap(Constants.Metrics.Tag.WORKFLOW_RUN_ID,
                                                             ProgramRunners.getRunId(programOptions).getId()),
          secureStore, secureStoreManager, messagingService, pluginInstantiator, metadataReader, metadataPublisher,
          namespaceQueryAdmin);
    this.workflowSpec = workflowSpec;
    this.conditionSpecification = conditionSpecification;
    this.token = token;
    this.nodeStates = nodeStates;
  }

  @Override
  public WorkflowSpecification getWorkflowSpecification() {
    return workflowSpec;
  }

  @Override
  public ConditionSpecification getConditionSpecification() {
    if (conditionSpecification == null) {
      throw new UnsupportedOperationException("Operation not allowed.");
    }
    return conditionSpecification;
  }

  @Override
  public WorkflowToken getToken() {
    return token;
  }

  @Override
  public Map<String, WorkflowNodeState> getNodeStates() {
    return ImmutableMap.copyOf(nodeStates);
  }

  /**
   * Sets the current state of the program.
   */
  void setState(ProgramState state) {
    this.state = state;
  }

  @Override
  public ProgramState getState() {
    return state;
  }

  boolean fieldLineageConsolidationEnabled() {
    return this.consolidateFieldOperations;
  }

  @Override
  public void enableFieldLineageConsolidation() {
    this.consolidateFieldOperations = true;
  }
}
