/*
 * Copyright 2020 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infai.ses.senergy.serialization;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;

public final class JSONSerdes {

    static public final class DeviceMessageSerde
            extends Serdes.WrapperSerde<DeviceMessageModel> {
        public DeviceMessageSerde() {
            super(new JSONSerializer<>(),
                    new JSONDeserializer<>(DeviceMessageModel.class));
        }
    }

    public static Serde<DeviceMessageModel> DeviceMessage() {
        return new JSONSerdes.DeviceMessageSerde();
    }

    static public final class AnalyticsMessageSerde
            extends Serdes.WrapperSerde<AnalyticsMessageModel> {
        public AnalyticsMessageSerde() {
            super(new JSONSerializer<>(),
                    new JSONDeserializer<>(AnalyticsMessageModel.class));
        }
    }

    public static Serde<AnalyticsMessageModel> AnalyticsMessage() {
        return new JSONSerdes.AnalyticsMessageSerde();
    }
}
