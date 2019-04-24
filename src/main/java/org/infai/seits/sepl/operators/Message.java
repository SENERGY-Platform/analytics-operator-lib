/*
 * Copyright 2018 InfAI (CC SES)
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

package org.infai.seits.sepl.operators;

import java.util.HashMap;
import java.util.Map;

public class Message {

    private String jsonMessage;
    private Map<String, Input> inputs = new HashMap<String, Input>();
    private Config config = new Config();

    public Message (){}

    public Message (String jsonMessage){
        this.jsonMessage = jsonMessage;
    }

    public Message setMessage (String message){
        this.jsonMessage = message;
        return this;
    }

    public Input addInput (String name){
        Input input = new Input(name, jsonMessage, this.config.inputTopic(name));
        this.inputs.put(name, input);
        return input;
    }

    public Input getInput (String name){
        return inputs.get(name).setMessage(this.jsonMessage);
    }

    public void output(String name, Object value){
        this.jsonMessage = Helper.setJSONPathValue(this.jsonMessage, "analytics."+name, value);
    }

    public String getMessageString(){
        return this.jsonMessage;
    }

    public void setConfig(String configString){
        this.config = new Config(configString);
    }
}
