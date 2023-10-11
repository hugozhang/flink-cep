/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.cep.events;

import java.util.Objects;

public class LoginEvent {

    private String eventType;
    private int userId;

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    private long eventTime;

    public LoginEvent() {

    }

    public LoginEvent(int userId, String eventType, long eventTime) {
        this.userId = userId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public int getUserId() {
        return userId;
    }

    public String getEventType() {
        return eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    @Override
    public String toString() {
        return ("Event(" + userId + ", " + eventType + ")");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LoginEvent) {
            LoginEvent other = (LoginEvent) obj;

            return eventType.equals(other.eventType) && userId == other.userId;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, userId);
    }
}

