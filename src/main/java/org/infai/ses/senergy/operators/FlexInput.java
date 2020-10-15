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

package org.infai.ses.senergy.operators;

import org.infai.ses.senergy.models.InputTopicModel;
import org.infai.ses.senergy.models.MappingModel;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.MessageProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlexInput {

    private String messageString = MessageProvider.getMessage().getMessageString();
    private final Config config = ConfigProvider.getConfig();
    private Map<String, Input> inputs = new HashMap<>();

    public FlexInput(String name) {
        // Check in inputtopic list if flex input was given
        for (InputTopicModel inputTopic : this.config.getInputTopicsConfigs()){
            for (MappingModel mapping : inputTopic.getMappings()){
                if (mapping.getDest().substring(0, mapping.getDest().lastIndexOf("_")).equals(name)){
                    inputs.put(mapping.getDest(),new Input(mapping.getDest()));
                }
            }
        }
    }

    public FlexInput setMessage(String message) {
        this.messageString = message;
        return this;
    }

    public List<Double> getValues() {
        ArrayList<Double> values = new ArrayList<>();
        for (Input input : this.inputs.values()) {
            values.add(input.setMessage(this.messageString).getValue());
        }
        return values;
    }
}
