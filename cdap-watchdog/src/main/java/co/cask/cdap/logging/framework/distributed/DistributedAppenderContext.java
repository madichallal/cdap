/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework.distributed;

import co.cask.cdap.api.logging.AppenderContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.logging.framework.AbstractAppenderContext;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A {@link AppenderContext} used in distributed mode.
 */
public class DistributedAppenderContext extends AbstractAppenderContext {

  /**
   * Constant for defining the named binding for the instance id field.
   */
  public static final String INSTANCE_ID_NAME = "appender.instanceId";

  private final int instanceId;

  @Inject
  DistributedAppenderContext(DatasetFramework datasetFramework,
                             TransactionSystemClient txClient,
                             LocationFactory locationFactory,
                             MetricsCollectionService metricsCollectionService,
                             @Named(INSTANCE_ID_NAME) int instanceId) {
    super(datasetFramework, txClient, locationFactory, metricsCollectionService);
    this.instanceId = instanceId;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }
}
