/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto.security;

import co.cask.cdap.proto.id.EntityId;
import com.google.common.base.Preconditions;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Request for revoking a user's permission to perform certain actions on an entity.
 */
public class RevokeRequest {

  private final EntityId entity;
  private final String user;
  private final Set<Action> actions;

  public RevokeRequest(EntityId entity, @Nullable String user, @Nullable Set<Action> actions) {
    Preconditions.checkArgument(entity != null, "entity is required");
    Preconditions.checkArgument(actions == null || (user != null), "user is required when actions is provided");
    this.entity = entity;
    this.user = user;
    this.actions = actions;
  }

  public EntityId getEntity() {
    return entity;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public Set<Action> getActions() {
    return actions;
  }
}
