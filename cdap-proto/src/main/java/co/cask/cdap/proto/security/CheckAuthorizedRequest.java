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

/**
 * Request for checking if a user can perform certain actions on an entity.
 */
public class CheckAuthorizedRequest {

  private final EntityId entity;
  private final String user;
  private final Set<Action> actions;

  public CheckAuthorizedRequest(EntityId entity, String user, Set<Action> actions) {
    Preconditions.checkArgument(entity != null, "entity is required");
    Preconditions.checkArgument(user != null, "user is required");
    Preconditions.checkArgument(actions != null, "actions is required");
    this.entity = entity;
    this.user = user;
    this.actions = actions;
  }

  public CheckAuthorizedRequest(CheckAuthorizedRequest other) {
    this(other.entity, other.user, other.actions);
  }

  public EntityId getEntity() {
    return entity;
  }

  public String getUser() {
    return user;
  }

  public Set<Action> getActions() {
    return actions;
  }
}
