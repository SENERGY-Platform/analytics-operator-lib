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

import org.infai.ses.senergy.models.*;
import org.infai.ses.senergy.utils.ConfigProvider;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Message {

    private static final Logger log = Logger.getLogger(Config.class.getName());

    private MessageModel message = new MessageModel();
    private final Map<String, Input> inputs = new HashMap<>();
    private final Map<String, FlexInput> flexInputs = new HashMap<>();
    private final Config config = ConfigProvider.getConfig();

    public Message setMessage (MessageModel message){
        this.message = message;
        this.parseMessage();
        return this;
    }

    public MessageModel getMessage(){
        return this.message;
    }

    public void addInput (String name){
        Input input = new Input();
        InputTopicModel topic = this.config.getInputTopicByDestination(name);
        if (topic != null){
            input.setSource(topic.getSourceByDest(name));
            input.setInputTopicName(topic.getName());
            this.inputs.put(name, input);
        } else {
            log.log(Level.INFO, "Missing config for input: " + name);
        }
    }

    public FlexInput addFlexInput (String name){
        FlexInput flexInput = new FlexInput(name);
        this.flexInputs.put(name, flexInput);
        return flexInput;
    }

    public Input getInput (String name){
        return this.inputs.get(name);
    }

    public FlexInput getFlexInput (String name){
        return new FlexInput("test");
    }

    public void output(String name, Object value){
        this.message.getOutputMessage().getAnalytics().put(name, value);
    }

    private void parseMessage() {
        for (Map.Entry<String, Input> entry  : this.inputs.entrySet()){
            Input input = entry.getValue();
            List<String> tree = new ArrayList<>(Arrays.asList(input.getSource().split("\\.")));
            InputMessageModel msg = this.message.getMessage(entry.getValue().getInputTopicName());
            if (tree.size() > 1) {
                tree.remove(0);
            }
            if (msg != null) {
                input.setValue(this.parse(msg.getValue(), tree));
                input.setFilterId(msg.getFilterIdFirst()+"-"+msg.getFilterIdSecond());
            } else {
                input.setValue(null);
                input.setFilterId(null);
                log.log(Level.INFO, "No value for input: " + input.getSource());
            }
        }
    }

    private Object parse (Map<String, Object> map, List<String> tree){
        for (String t : tree){
            if (map.get(t) instanceof  Map<?, ?>){
                map = (Map<String, Object>) map.get(t);
            } else {
                return map.get(t);
            }
        }
        return map;
    }
}