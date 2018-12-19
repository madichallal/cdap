/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.profile;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Class for publishing the profile metadata change request to tms
 */
public class AdminEventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(AdminEventPublisher.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
    new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())).create();

  private final TopicId topic;
  private final RetryStrategy retryStrategy;
  private final MessagingContext messagingContext;

  public AdminEventPublisher(CConfiguration cConf, MessagingContext messagingContext) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC));
    this.retryStrategy = RetryStrategies.timeLimit(
      20, TimeUnit.SECONDS,
      RetryStrategies.exponentialDelay(10, 200, TimeUnit.MILLISECONDS));
    this.messagingContext = messagingContext;
  }

  /**
   * Update the profile metadata for the affected programs/schedules, the metadata of programs/schedules will be
   * updated according to the closest level preference that contains the profile information.
   *
   * @param entityId the entity id to update metadata, all programs/schedules under this entity id will be affected
   * @param profileId the profileId which get assigned
   */
  public void publishProfileAssignment(EntityId entityId, ProfileId profileId) {
    publishMessage(entityId, MetadataMessage.Type.PROFILE_ASSIGNMENT, profileId);
  }

  /**
   * Publish a message when a profile is unassigned to an entity. The metadata of the programs/schedules will be
   * reindexed to the closest level preference that contains the profile information
   *
   * @param entityId the entity id to reindex metadata, all programs/schedules under this entity id will be affected
   */
  public void publishProfileUnAssignment(EntityId entityId) {
    publishMessage(entityId, MetadataMessage.Type.PROFILE_UNASSIGNMENT);
  }

  /**
   * Publish a message about app creation, the programs/schedules which get created will get updated with the profile
   * metadata
   *
   * @param applicationId the app id that get created
   * @param appSpec the application specification of the app
   */
  public void publishAppCreation(ApplicationId applicationId, ApplicationSpecification appSpec) {
    publishMessage(applicationId, MetadataMessage.Type.ENTITY_CREATION, appSpec);
  }

  /**
   * Publish a message about schedule creation, the schedules which get created will get updated with the profile
   * metadata
   *
   * @param scheduleId the schedule id that get created
   */
  public void publishScheduleCreation(ScheduleId scheduleId) {
    publishMessage(scheduleId, MetadataMessage.Type.ENTITY_CREATION);
  }

  /**
   * Remove the profile metadata for the given app id. Note that the app spec is required since we might not
   * find the program/schedule information from the app meta store since they can be deleted.
   * So we must specify app spec here to make sure their profile metadata is removed.
   *
   * @param appId entity id who emits the message
   * @param appSpec the appSpec of the app
   */
  public void publishAppDeletion(ApplicationId appId, ApplicationSpecification appSpec) {
    publishMessage(appId, MetadataMessage.Type.ENTITY_DELETION, appSpec);
  }

  /**
   * Remove the profile metadata for the given schedule id. Note that the ProgramSchedule is needed since the schedule
   * might already get deleted when we process the message. So we must specify it to make sure the profile metadata is
   * removed.
   *
   * @param scheduleId schedule id who gets deleted
   * @param programSchedule the detail of this schedule
   */
  public void publishScheduleDeletion(ScheduleId scheduleId, ProgramSchedule programSchedule) {
    publishMessage(scheduleId, MetadataMessage.Type.ENTITY_DELETION, programSchedule);
  }

  private void publishMessage(EntityId entityId, MetadataMessage.Type type) {
    publishMessage(entityId, type, JsonNull.INSTANCE);
  }

  private void publishMessage(EntityId entityId, MetadataMessage.Type type,
                              Object payload) {
    MetadataMessage message = new MetadataMessage(type, entityId, GSON.toJsonTree(payload));
    LOG.trace("Publishing message: {}", message);
    try {
      Retries.supplyWithRetries(
        () -> {
          try {
            messagingContext.getMessagePublisher().publish(NamespaceId.SYSTEM.getNamespace(), topic.getTopic(),
                                                           GSON.toJson(message));
          } catch (TopicNotFoundException | ServiceUnavailableException e) {
            throw new RetryableException(e);
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
          return null;
        },
        retryStrategy);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to publish profile metadata request for entity id %s",
                                               entityId), e);
    }
  }
}
