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

package org.infai.ses.senergy.models;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class ConfigModel {

    private List<InputTopicModel> inputTopics = new LinkedList<>();
    private Map<String, String> config = new LinkedHashMap<>();

    public ConfigModel(){
    }

    public ConfigModel(List<InputTopicModel> inputTopics, Map<String, String> config){
        this.inputTopics = inputTopics;
        this.config = config;
    }

    public List<InputTopicModel> getInputTopics() {
        return this.inputTopics;
    }

    public Map<String, String> getOperatorConfig(){ return this.config; }
}
