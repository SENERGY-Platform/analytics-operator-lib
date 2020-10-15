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

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.common.protocol.types.Field;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.HashMap;
import java.util.Map;

public class Message {

    private String jsonMessage;
    private final Map<String, Input> inputs = new HashMap<String, Input>();
    private final Map<String, FlexInput> flexInputs = new HashMap<String, FlexInput>();
    private final Config config = ConfigProvider.getConfig();
    private final String deviceIdPath = Helper.getEnv("DEVICE_ID_PATH", "device_id");
    private final String pipelineIDPath = Helper.getEnv("PIPELINE_ID_PATH", "pipeline_id");

    public Message (){}

    public Message (String jsonMessage){
        this.jsonMessage = jsonMessage;
    }

    public Message setMessage (String message){
        this.jsonMessage = message;
        return this;
    }

    public Input addInput (String name){
        Input input = new Input(name);
        this.inputs.put(name, input);
        return input;
    }

    public FlexInput addFlexInput (String name){
        FlexInput flexInput = new FlexInput(name);
        this.flexInputs.put(name, flexInput);
        return flexInput;
    }

    public Input getInput (String name){
        return inputs.get(name).setMessage(this.jsonMessage);
    }

    public FlexInput getFlexInput (String name){
        return flexInputs.get(name).setMessage(this.jsonMessage);
    }
    
    public String getMessageEntityId(){
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < this.config.getTopicConfig().length(); i++) {
            switch (((org.json.JSONObject)this.config.getTopicConfigById(i).get(0)).get(Values.FILTER_TYPE_KEY).toString()) {
                case Values.FILTER_TYPE_OPERATOR_KEY:
                    String PIPE_ID_PATH = "$.inputs["+ i+"]." + pipelineIDPath;
                    if (Helper.checkPathExists(this.jsonMessage, PIPE_ID_PATH)) {
                        id.append((String) JsonPath.parse(this.jsonMessage).read(PIPE_ID_PATH));
                    }
                    break;
                case Values.FILTER_TYPE_DEVICE_KEY:
                    String DEVICE_ID_PATH = "$.inputs["+ i+"]." + deviceIdPath;
                    if (Helper.checkPathExists(this.jsonMessage, DEVICE_ID_PATH)) {
                        id.append((String) JsonPath.parse(this.jsonMessage).read(DEVICE_ID_PATH));
                    }
                    break;
                default:
                    break;
            }
            if (this.config.getTopicConfig().length() > 1 && i < this.config.getTopicConfig().length()-1){
                id.append(",");
            }
        }
        return id.toString();
    }

    public void output(String name, Object value){
        this.jsonMessage = Helper.setJSONPathValue(this.jsonMessage, "analytics."+name, value);
    }

    public String getMessageString(){
        return this.jsonMessage;
    }
}
