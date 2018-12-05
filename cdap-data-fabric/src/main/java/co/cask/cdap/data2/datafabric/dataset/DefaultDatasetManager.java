/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link DatasetManager} that performs operation via {@link DatasetFramework}.
 */
public class DefaultDatasetManager implements DatasetManager {

  private final DatasetFramework datasetFramework;
  private final NamespaceId namespaceId;
  private final RetryStrategy retryStrategy;
  @Nullable
  private final KerberosPrincipalId principalId;

  /**
   * Constructor.
   * @param datasetFramework the {@link DatasetFramework} to use for performing the actual operation
   * @param namespaceId the {@link NamespaceId} for all dataset managed through this class
   * @param retryStrategy the {@link RetryStrategy} to use for {@link RetryableException}.
   * @param principalId the {@link KerberosPrincipalId} for all datasets created.
   */
  public DefaultDatasetManager(DatasetFramework datasetFramework,
                               NamespaceId namespaceId,
                               RetryStrategy retryStrategy,
                               @Nullable KerberosPrincipalId principalId) {
    this.datasetFramework = datasetFramework;
    this.namespaceId = namespaceId;
    this.retryStrategy = retryStrategy;
    this.principalId = principalId;
  }

  @Override
  public boolean datasetExists(String name) throws DatasetManagementException {
    return datasetExists(namespaceId.dataset(name));
  }

  @Override
  public boolean datasetExists(String namespace, String name) throws DatasetManagementException {
    return datasetExists(new NamespaceId(namespace).dataset(name));
  }

  private boolean datasetExists(DatasetId datasetId) throws DatasetManagementException {
    return Retries.callWithRetries(() -> datasetFramework.getDatasetSpec(datasetId) != null, retryStrategy);
  }

  @Override
  public String getDatasetType(String name) throws DatasetManagementException {
    return getDatasetType(namespaceId.dataset(name));
  }

  @Override
  public String getDatasetType(String namespace, String name) throws DatasetManagementException {
    return getDatasetType(new NamespaceId(namespace).dataset(name));
  }

  @Override
  public DatasetProperties getDatasetProperties(String name) throws DatasetManagementException {
    return getDatasetProperties(namespaceId.dataset(name));
  }

  @Override
  public DatasetProperties getDatasetProperties(String namespace, String name) throws DatasetManagementException {
    return getDatasetProperties(new NamespaceId(namespace).dataset(name));
  }

  @Override
  public void createDataset(String name, String type,
                            DatasetProperties properties) throws DatasetManagementException {
    createDataset(namespaceId.dataset(name), type, properties);
  }

  @Override
  public void createDataset(String namespace, String name, String type,
                            DatasetProperties properties) throws DatasetManagementException {
    createDataset(new NamespaceId(namespace).dataset(name), type, properties);
  }

  @Override
  public void updateDataset(String name, DatasetProperties properties) throws DatasetManagementException {
    updateDataset(namespaceId.dataset(name), properties);
  }

  @Override
  public void updateDataset(String namespace, String name,
                            DatasetProperties properties) throws DatasetManagementException {
    updateDataset(new NamespaceId(namespace).dataset(name), properties);
  }

  @Override
  public void dropDataset(String name) throws DatasetManagementException {
    dropDataset(namespaceId.dataset(name));
  }

  @Override
  public void dropDataset(String namespace, String name) throws DatasetManagementException {
    dropDataset(new NamespaceId(namespace).dataset(name));
  }

  @Override
  public void truncateDataset(final String name) throws DatasetManagementException {
    truncateDataset(namespaceId.dataset(name));
  }

  @Override
  public void truncateDataset(String namespace, String name) throws DatasetManagementException {
    truncateDataset(new NamespaceId(namespace).dataset(name));
  }

  private DatasetProperties getDatasetProperties(DatasetId datasetId) throws DatasetManagementException {
    return Retries.callWithRetries(() -> {
      DatasetSpecification spec = datasetFramework.getDatasetSpec(datasetId);
      if (spec == null) {
        throw new InstanceNotFoundException(datasetId.getEntityName());
      }
      return DatasetProperties.of(spec.getOriginalProperties());
    }, retryStrategy);
  }

  private String getDatasetType(DatasetId datasetId) throws DatasetManagementException {
    return Retries.callWithRetries(() -> {
      DatasetSpecification spec = datasetFramework.getDatasetSpec(datasetId);
      if (spec == null) {
        throw new InstanceNotFoundException(datasetId.getDataset());
      }
      return spec.getType();
    }, retryStrategy);
  }

  private void createDataset(DatasetId datasetId, String type,
                             DatasetProperties properties) throws DatasetManagementException {
    Retries.runWithRetries(() -> {
      try {
        // we have to do this check since addInstance method can only be used when app impersonation is enabled
        if (principalId != null) {
          datasetFramework.addInstance(type, datasetId, properties, principalId);
        } else {
          datasetFramework.addInstance(type, datasetId, properties);
        }
      } catch (IOException ioe) {
        // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
        throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                           datasetId.getDataset(), ioe.getMessage()), ioe);
      }
    }, retryStrategy);
  }

  private void updateDataset(DatasetId datasetId, DatasetProperties properties) throws DatasetManagementException {
    Retries.runWithRetries(() -> {
      try {
        datasetFramework.updateInstance(datasetId, properties);
      } catch (IOException ioe) {
        // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
        throw new DatasetManagementException(String.format("Failed to update instance %s, details: %s",
                                                           datasetId.getDataset(), ioe.getMessage()), ioe);
      }
    }, retryStrategy);
  }

  private void dropDataset(DatasetId datasetId) throws DatasetManagementException {
    Retries.runWithRetries(() -> {
      try {
        datasetFramework.deleteInstance(datasetId);
      } catch (IOException ioe) {
        // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
        throw new DatasetManagementException(String.format("Failed to delete instance %s, details: %s",
                                                           datasetId.getDataset(), ioe.getMessage()), ioe);
      }
    }, retryStrategy);
  }

  private void truncateDataset(DatasetId datasetId) throws DatasetManagementException {
    Retries.runWithRetries(() -> {
      try {
        datasetFramework.truncateInstance(datasetId);
      } catch (IOException ioe) {
        // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
        throw new DatasetManagementException(String.format("Failed to truncate instance %s, details: %s",
                                                           datasetId.getDataset(), ioe.getMessage()), ioe);
      }
    }, retryStrategy);
  }
}
