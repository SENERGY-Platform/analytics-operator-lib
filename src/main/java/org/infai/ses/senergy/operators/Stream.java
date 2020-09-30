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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.StreamsConfigProvider;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class Stream {


    final Serde<String> stringSerde = Serdes.String();
    private KStream<String, String> outputData;
    private final String deviceIdPath = Helper.getEnv("DEVICE_ID_PATH", "device_id");
    private final String pipelineIDPath = Helper.getEnv("PIPELINE_ID_PATH", "pipeline_id");
    private String pipelineId = Helper.getEnv("PIPELINE_ID", "");
    private String operatorId = Helper.getEnv("OPERATOR_ID", "");
    private final Integer windowTime = Helper.getEnv("WINDOW_TIME", Values.WINDOW_TIME);
    private final Boolean DEBUG = Boolean.valueOf(Helper.getEnv("DEBUG", "false"));
    private final String operatorIdPath = "operator_id";
    private final Boolean resetApp = Boolean.valueOf(Helper.getEnv("RESET_APP", "false"));
    private final Boolean kTableProcessing = Boolean.valueOf(Helper.getEnv("KTABLE_PROCESSING", "true"));

    final private Message message = new Message();
    private Config config = ConfigProvider.getConfig();

    private KafkaStreams streams;

    public StreamBuilder streamBuilder;
    public TableBuilder tableBuilder;

    public Stream() {
        streamBuilder = new StreamBuilder(operatorId, pipelineId);
        tableBuilder = new TableBuilder(operatorId, pipelineId);
    }

    public Stream(String operatorId, String pipelineId) {
        this.operatorId = operatorId;
        this.pipelineId = pipelineId;
        this.streamBuilder = new StreamBuilder(operatorId, pipelineId);
        this.streamBuilder.setWindowTime(windowTime);
        this.tableBuilder = new TableBuilder(operatorId, pipelineId);
    }

    /**
     * Start the streams application.
     *
     * @param operator OperatorInterface
     */
    public void start(OperatorInterface operator) {
        operator.configMessage(message);
        if (config.topicCount() > 1) {
            if (kTableProcessing) {
                processMultipleStreamsAsTable(operator, config.getTopicConfig());
            } else {
                processMultipleStreams(operator, config.getTopicConfig());
            }
        } else if (config.topicCount() == 1) {
            processSingleStream(operator, config.getTopicConfig());
        }
        streams = new KafkaStreams(streamBuilder.getBuilder().build(), StreamsConfigProvider.getStreamsConfiguration());
        if (resetApp){
            streams.cleanUp();
        }
        
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Close the streams application.
     */
    public void closeStreams(){
        streams.close();
    }

    /**
     * Process a single stream as a record stream.
     *
     * @param operator OperatorInterface
     * @param topicConfig JSONArray
     */
    public void processSingleStream(OperatorInterface operator, JSONArray topicConfig) {
        JSONObject topic = new JSONObject(topicConfig.get(0).toString());

        KStream<String, String> inputData = streamBuilder.getBuilder().stream(topic.getString(Values.TOPIC_NAME_KEY));
        //Filter Stream
        KStream<String, String> filterData = filterStream(topic, inputData);


        if (DEBUG) {
            filterData.print(Printed.toSysOut());
        }

        //format message
        filterData = filterData.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, streamBuilder.formatMessage(value)));
            return result;
        });

        //output stream
        outputStream(operator, filterData);
    }

    /**
     * Process a single stream as a changelog stream.
     *
     * @param operator OperatorInterface
     * @param topicConfig JSONArray
     */
    public void processSingleStreamAsTable (OperatorInterface operator, JSONArray topicConfig){
        JSONObject topic = new JSONObject(topicConfig.get(0).toString());

        KTable<String, String> inputData = streamBuilder.getBuilder().table(topic.getString(Values.TOPIC_NAME_KEY));
        //Filter Stream
        KTable<String, String> filterData = filterStream(topic, inputData);


        if (DEBUG) {
            filterData.toStream().print(Printed.toSysOut());
        }

        //format message
        KStream<String, String> outputStream = filterData.toStream().flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, streamBuilder.formatMessage(value)));
            return result;
        });

        //output stream
        outputStream(operator, outputStream);
    }

    /**
     * Processes multiple streams as a record stream while automatically creating only one inputStream per topic.
     *
     * @param operator OperatorInterface
     * @param topicConfig JSONArray
     */
    public void processMultipleStreams(OperatorInterface operator, JSONArray topicConfig) {
        int length = topicConfig.length();
        Map<String, KStream<String, String>> inputStreamsMap = new HashMap<>();
        KStream<String, String>[] filterData = new KStream[length];
        for(int i = 0; i < length; i++){
            JSONObject topic = new JSONObject(topicConfig.get(i).toString());
            String topicName = topic.getString(Values.TOPIC_NAME_KEY);

            if(!inputStreamsMap.containsKey(topicName)){
                //no inputStream for topic created yet
                inputStreamsMap.put(topicName, streamBuilder.getBuilder().stream(topicName));
            }

            filterData[i] = filterStream(topic, inputStreamsMap.get(topicName));

            if (DEBUG) {
                filterData[i].print(Printed.toSysOut());
            }

            filterData[i] = filterData[i].flatMap((key, value) -> {
                List<KeyValue<String, String>> result = new LinkedList<>();
                result.add(KeyValue.pair("A", value));
                return result;
            });
        }
        KStream<String, String> merged = streamBuilder.joinMultipleStreams(filterData);
        outputStream(operator, merged);
    }

    /**
     * Processes multiple streams as a changelog stream while automatically creating only one inputStream per topic.
     *
     * @param operator OperatorInterface
     * @param topicConfig JSONArray
     */
    public void processMultipleStreamsAsTable(OperatorInterface operator, JSONArray topicConfig) {
        int length = topicConfig.length();
        Map<String, KTable<String, String>> inputStreamsMap = new HashMap<>();
        KTable<String, String>[] filterData = new KTable[length];
        for(int i = 0; i < length; i++){
            JSONObject topic = new JSONObject(topicConfig.get(i).toString());
            String topicName = topic.getString(Values.TOPIC_NAME_KEY);

            if(!inputStreamsMap.containsKey(topicName)){
                //no inputStream for topic created yet
                inputStreamsMap.put(topicName, streamBuilder.getBuilder().table(topicName));
            }

            filterData[i] = filterStream(topic, inputStreamsMap.get(topicName));

            if (DEBUG) {
                filterData[i].toStream().print(Printed.toSysOut());
            }

            filterData[i] = filterData[i].toStream().flatMap((key, value) -> {
                List<KeyValue<String, String>> result = new LinkedList<>();
                result.add(KeyValue.pair("A", value));
                return result;
            }).toTable();
        }
        KTable<String, String> merged = tableBuilder.joinMultipleStreams(filterData);
        outputStream(operator, merged.toStream());
    }

    /**
     * Output a stream.
     *
     * @param operator OperatorInterface
     * @param outputStream KStream
     */
    private void outputStream(OperatorInterface operator, KStream<String, String> outputStream) {
        // Execute operator logic
        KStream<String, String> rawOutputStream = outputStream.flatMap((key,value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            operator.run(this.message.setMessage(value));
            result.add(KeyValue.pair(this.operatorId != null ? this.operatorId : this.pipelineId, this.message.getMessageString()));
            return result;
        });
        // check if output value was set or drop message
        outputData = rawOutputStream.filter((key, value) -> {
            JSONObject messageObj =  new JSONObject(value);
            JSONObject ana = new JSONObject(messageObj.get("analytics").toString());
            if(ana.length() >0){
                return true;
            }
            return false;
        });

        if (DEBUG) {
            outputData.print(Printed.toSysOut());
        }

        outputData.to(getOutputStreamName(), Produced.with(stringSerde, stringSerde));
    }

    /**
     * Filter the input stream as a record stream by operatorId or deviceId.
     *
     * @param topic JSONObject
     * @param inputData KStream
     * @return KStream
     */
    private KStream<String, String> filterStream(JSONObject topic, KStream<String, String> inputData) {
        KStream<String, String> filterData;
        String[] filterValues  = topic.getString(Values.FILTER_VALUE_KEY).split(",");
        switch (topic.getString(Values.FILTER_TYPE_KEY)) {
            case Values.FILTER_TYPE_OPERATOR_KEY:
                KStream<String, String> pipelineFilterData = streamBuilder.filterBy(inputData, pipelineIDPath, new String[]{pipelineId});
                filterData = streamBuilder.filterBy(pipelineFilterData, operatorIdPath, filterValues);
                break;
            case Values.FILTER_TYPE_DEVICE_KEY:
                filterData = streamBuilder.filterBy(inputData, deviceIdPath, filterValues);
                break;
            default:
                filterData = inputData;
                break;
        }
        return filterData;
    }

    /**
     * Filter the input stream as a changelog stream by operatorId or deviceId.
     *
     * @param topic JSONObject
     * @param inputData KTable
     * @return KTable
     */
    private KTable<String, String> filterStream(JSONObject topic, KTable<String, String> inputData) {
        KTable<String, String> filterData;
        String[] filterValues  = topic.getString(Values.FILTER_VALUE_KEY).split(",");
        switch (topic.getString(Values.FILTER_TYPE_KEY)) {
            case Values.FILTER_TYPE_OPERATOR_KEY:
                KTable<String, String> pipelineFilterData = tableBuilder.filterBy(inputData, pipelineIDPath, new String[]{pipelineId});
                filterData = tableBuilder.filterBy(pipelineFilterData, operatorIdPath, filterValues);
                break;
            case Values.FILTER_TYPE_DEVICE_KEY:
                filterData = tableBuilder.filterBy(inputData, deviceIdPath, filterValues);
                break;
            default:
                filterData = inputData;
                break;
        }
        return filterData;
    }

    public KStream<String, String> getOutputStream() {
        return outputData;
    }

    public String getOutputStreamName() {
        return Helper.getEnv("OUTPUT", "output-stream");
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }
}
