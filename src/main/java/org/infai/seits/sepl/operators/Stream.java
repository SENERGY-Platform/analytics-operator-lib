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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Stream {


    final Serde<String> stringSerde = Serdes.String();
    private KStream<String, String> outputData;
    private String deviceIdPath = Helper.getEnv("DEVICE_ID_PATH", "device_id");
    private String pipelineIDPath = Helper.getEnv("PIPELINE_ID_PATH", "pipeline_id");
    private String pipelineId = Helper.getEnv("PIPELINE_ID", "");
    private String operatorId = Helper.getEnv("OPERATOR_ID", "");
    private Integer windowTime = Helper.getEnv("WINDOW_TIME", 100);
    private Boolean DEBUG = Boolean.valueOf(Helper.getEnv("DEBUG", "false"));
    private String operatorIdPath = "operator_id";

    final private Message message = new Message();

    private Config config = new Config();

    public Builder builder;

    public Stream(){
        builder = new Builder(operatorId, pipelineId);
    }

    public Stream(String operatorId, String pipelineId){
        this.operatorId = operatorId;
        this.pipelineId = pipelineId;
        this.builder = new Builder(operatorId, pipelineId);
        this.builder.setWindowTime(windowTime);
    }

    /**
     * Set config values for Stream processor.
     *
     * @return Properties streamsConfiguration
     */
    public static Properties config() {
        Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, Helper.getEnv("CONFIG_APPLICATION_ID", "stream-operator"));
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Helper.getEnv("CONFIG_BOOTSTRAP_SERVERS", Helper.getBrokerList(Helper.getEnv("ZK_QUORUM", "localhost:2181"))));
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        return streamsConfiguration;
    }

    public void start(OperatorInterface operator){
        operator.config(this.message);
        if (this.config.topicCount()  == 2) {
            if (this.config.getTopicName(0).equals(this.config.getTopicName(1))){
                processSingleStream2Filter(operator, this.config.getConfig());
            } else {
                processTwoStreams(operator, this.config.getConfig());
            }

        } else if (this.config.topicCount() == 1) {
            processSingleStream(operator, this.config.getConfig());
        }
        KafkaStreams streams = new KafkaStreams(builder.getBuilder().build(), Stream.config());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Process a single stream.
     *
     * @param operator
     */
    public void processSingleStream(OperatorInterface operator, JSONArray topicConfig) {


        JSONObject topic = new JSONObject(topicConfig.get(0).toString());
        KStream<String, String> inputData = builder.getBuilder().stream(topic.getString("Name"));

        //Filter Stream
        KStream<String, String> filterData = filterStream(topic, inputData);

        if(DEBUG){
            filterData.print(Printed.toSysOut());
        }

        outputData = filterData.flatMapValues(value -> {
            operator.run(this.message.setMessage(builder.formatMessage(value)));
            return Arrays.asList(this.message.getMessageString());
        });
        //Debug
        if(DEBUG){
            outputData.print(Printed.toSysOut());
        }
        outputData.to(getOutputStreamName(), Produced.with(stringSerde, stringSerde));
    }

    public void processSingleStream2Filter(OperatorInterface operator, JSONArray topicConfig){
        JSONObject topic1 = new JSONObject(topicConfig.get(0).toString());
        JSONObject topic2 = new JSONObject(topicConfig.get(1).toString());

        KStream<String, String> inputData = builder.getBuilder().stream(topic1.getString("Name"));

        // Filter streams
        KStream<String, String> filterData1 = filterStream(topic1, inputData);
        KStream<String, String> filterData2 = filterStream(topic2, inputData);

        if(DEBUG){
            filterData1.print(Printed.toSysOut());
            filterData2.print(Printed.toSysOut());
        }

        filterData1 = filterData1.flatMap((key, value)->{
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair("A", value));
            return result;
        });

        filterData2 = filterData2.flatMap((key, value)->{
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair("A", value));
            return result;
        });

        // Merge Streams
        KStream<String, String> merged = builder.joinStreams(filterData1, filterData2);

        outputData = merged.flatMapValues(value -> {
            operator.run(this.message.setMessage(value));
            return Arrays.asList(this.message.getMessageString());
        });

        if(DEBUG){
            outputData.print(Printed.toSysOut());
        }
        outputData.to(getOutputStreamName(), Produced.with(stringSerde, stringSerde));
    }

    public void processTwoStreams(OperatorInterface operator, JSONArray topicConfig) {
        JSONObject topic1 = new JSONObject(topicConfig.get(0).toString());
        JSONObject topic2 = new JSONObject(topicConfig.get(1).toString());

        KStream<String, String> inputStream1 = builder.getBuilder().stream(topic1.getString("Name"));
        KStream<String, String> inputStream2 = builder.getBuilder().stream(topic2.getString("Name"));

        // Filter streams
        KStream<String, String> filterData1 = filterStream(topic1, inputStream1);
        KStream<String, String> filterData2 = filterStream(topic2, inputStream2);

        if(DEBUG){
            filterData1.print(Printed.toSysOut());
            filterData2.print(Printed.toSysOut());
        }

        filterData1 = filterData1.flatMap((key, value)->{
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair("A", value));
            return result;
        });

        filterData2 = filterData2.flatMap((key, value)->{
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair("A", value));
            return result;
        });

        // Merge Streams
        KStream<String, String> merged = builder.joinStreams(filterData1, filterData2);

        outputData = merged.flatMapValues(value -> {
            operator.run(this.message.setMessage(value));
            return Arrays.asList(this.message.getMessageString());
        });

        if(DEBUG){
            outputData.print(Printed.toSysOut());
        }
        outputData.to(getOutputStreamName(), Produced.with(stringSerde, stringSerde));
    }

    private KStream<String,String> filterStream (JSONObject topic, KStream<String,String> inputData){
        KStream<String, String> filterData;
        switch (topic.getString("FilterType")){
            case "OperatorId":
                KStream<String, String> pipelineFilterData = builder.filterBy(inputData, pipelineIDPath, pipelineId);
                filterData = builder.filterBy(pipelineFilterData, operatorIdPath, topic.getString("FilterValue"));
                break;
            case "DeviceId":
                filterData = builder.filterBy(inputData, deviceIdPath, topic.getString("FilterValue"));
                break;
            default:
                filterData = inputData;
                break;
        }
        return filterData;
    }

    public KStream<String, String> getOutputStream(){
        return outputData;
    }

    public String getOutputStreamName() {
        return Helper.getEnv("OUTPUT", "output-stream");
    }

    public void setPipelineId(String pipelineId){
        this.pipelineId = pipelineId;
    }
}
