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
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.infai.ses.senergy.operators.Values;

import java.util.HashMap;
import java.util.Map;

public class MessageModel{

    private final Map<String, InputMessageModel> inputMessages = new HashMap<>();
    private final AnalyticsMessageModel outputMessage = new AnalyticsMessageModel();

    @JsonIgnore
    private long kafkaTimestamp = -1;

    public MessageModel(){
        this.outputMessage.setOperatorId(Values.OPERATOR_ID);
        this.outputMessage.setPipelineId(Values.PIPELINE_ID);
        this.outputMessage.setAnalytics(new HashMap<>());
    }

    public void putMessage( String topicName, InputMessageModel  value ) {
        this.inputMessages.put( topicName, value);
    }

    public InputMessageModel getMessage( String topicName ) {
        return this.inputMessages.get( topicName );
    }

    public Map<String, InputMessageModel> getMessages(){
        return this.inputMessages;
    }

    public AnalyticsMessageModel getOutputMessage(){
        return this.outputMessage;
    }

    public void setProcessed(){
        for (Map.Entry<String, InputMessageModel> entry: this.inputMessages.entrySet()){
            entry.getValue().setProcessed();
        }
    }

    @JsonIgnore
    public void setKafkaTimestamp(long timestamp){
        this.kafkaTimestamp = timestamp;
    }
    @JsonIgnore
    public long getKafkaTimestamp(){
        return this.kafkaTimestamp;
    }
}
