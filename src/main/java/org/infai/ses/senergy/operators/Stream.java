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
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.InputTopicModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.serialization.JSONSerdes;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.StreamsConfigProvider;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class Stream {

    final Serde<String> stringSerde = Serdes.String();
    private KStream<String, String> outputStream;
    private final Integer windowTime = Helper.getEnv("WINDOW_TIME", Values.WINDOW_TIME);
    private static final Boolean DEBUG = Boolean.valueOf(Helper.getEnv("DEBUG", "false"));
    private static final String operatorIdPath = "operator_id";
    private final Boolean resetApp = Boolean.valueOf(Helper.getEnv("RESET_APP", "false"));
    private final Boolean kTableProcessing = Boolean.valueOf(Helper.getEnv("KTABLE_PROCESSING", "true"));

    private static OperatorInterface operator;

    private Message message = new Message();
    private final Config config = ConfigProvider.getConfig();

    private KafkaStreams streams;

    public StreamBuilder streamBuilder;
    public TableBuilder tableBuilder;

    public Stream() {
        streamBuilder = new StreamBuilder();
        this.streamBuilder.setWindowTime(windowTime);
        tableBuilder = new TableBuilder();
    }

    /**
     * Start the streams application.
     *
     * @param runOperator OperatorInterface
     */
    public void start(OperatorInterface runOperator) {
        operator = runOperator;
        message = operator.configMessage(message);
        if (config.topicCount() > 1) {
            if (Boolean.TRUE.equals(kTableProcessing)) {
                processMultipleStreamsAsTable(config.getTopicConfig());
            } else {
                processMultipleStreams(config.getInputTopicsConfigs());
            }
        } else if (config.topicCount() == 1) {
            processSingleStream(config.getInputTopicsConfigs().get(0));
        }
        streams = new KafkaStreams(streamBuilder.getBuilder().build(), StreamsConfigProvider.getStreamsConfiguration());
        if (Boolean.TRUE.equals(resetApp)){
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
     * @param topicConfig JSONArray
     */
    public void processSingleStream(InputTopicModel topicConfig) {
        KStream<String, MessageModel> messagesStream = parseInputStream(topicConfig);

        if (Boolean.TRUE.equals(DEBUG)) {
            messagesStream.print(Printed.toSysOut());
        }

        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(messagesStream);
        dropEmptyMessages();
        outputStream(afterOperatorStream);
    }

    /**
     * Process a single stream as a changelog stream.
     *
     * @param topicConfig JSONArray
     */
    public void processSingleStreamAsTable (JSONArray topicConfig){
        /**
        JSONObject topic = new JSONObject(topicConfig.get(0).toString());

        KTable<String, String> inputData = streamBuilder.getBuilder().table(topic.getString(Values.TOPIC_NAME_KEY));
        //Filter Stream
        KTable<String, String> filteredStream = filterStream(topic, inputData);

        if (Boolean.TRUE.equals(DEBUG)) {
            filteredStream.toStream().print(Printed.toSysOut());
        }

        //format message
        KStream<String, String> outputStream = filteredStream.toStream().flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, streamBuilder.formatMessage(value)));
            return result;
        });

        //runOperatorLogic(outputStream);
        //dropEmptyMessages();
        //outputStream();
         **/
    }

    /**
     * Processes multiple streams as a record stream while automatically creating only one inputStream per topic.
     *
     * @param topicConfigs List<InputTopicModel>
     */
    public void processMultipleStreams(List<InputTopicModel> topicConfigs) {
        /*
        int length = topicConfigs.size();
        Map<String, KStream<String, MessageModel>> inputStreamsMap = new HashMap<>();
        KStream<String, MessageModel>[] filterData = new KStream[length];
        for(int i = 0; i < length; i++){
            InputTopicModel topicConfig = topicConfigs.get(i);
            String topicName = topicConfig.getName();
            KStream<String, MessageModel> messagesStream = parseInputStream(topicConfig);

            if(!inputStreamsMap.containsKey(topicName)){
                //no inputStream for topic created yet
                inputStreamsMap.put(topicName, messagesStream);
            }

            filterData[i] = filterStream(topicConfig, inputStreamsMap.get(topicName));

            if (Boolean.TRUE.equals(DEBUG)) {
                filterData[i].print(Printed.toSysOut());
            }

            filterData[i] = filterData[i].flatMap((key, value) -> {
                List<KeyValue<String, MessageModel>> result = new LinkedList<>();
                result.add(KeyValue.pair("A", value));
                return result;
            });
        }
        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(streamBuilder.joinMultipleStreams(filterData));
        dropEmptyMessages();
        outputStream(afterOperatorStream);
        */
    }

    /**
     * Processes multiple streams as a changelog stream while automatically creating only one inputStream per topic.
     *
     * @param topicConfig JSONArray
     */
    public void processMultipleStreamsAsTable(JSONArray topicConfig) {
        /*
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

            if (Boolean.TRUE.equals(DEBUG)) {
                filterData[i].toStream().print(Printed.toSysOut());
            }

            filterData[i] = filterData[i].toStream().flatMap((key, value) -> {
                List<KeyValue<String, String>> result = new LinkedList<>();
                result.add(KeyValue.pair("A", value));
                return result;
            }).toTable();
        }
        KTable<String, String> merged = tableBuilder.joinMultipleStreams(filterData);
        runOperatorLogic(merged.toStream());
        dropEmptyMessages();
        outputStream();
        */
    }

    public KStream<String, String> getOutputStream() {
        return outputStream;
    }

    /**
     * Returns the name of the output topic.
     *
     * @return String
     */
    public String getOutputStreamName() {
        return Helper.getEnv("OUTPUT", "output-stream");
    }

    /**
     * Sets the running operator.
     *
     * @param runOperator OperatorInterface
     */
    public void setOperator(OperatorInterface runOperator){
        operator = runOperator;
    }

    /**
     * Output the stream.
     */
    private void outputStream(KStream<String, MessageModel> inputStream) {
        outputStream = inputStream.flatMap((key, value) -> {
            List<KeyValue<String, String>> result= new LinkedList<>();
            result.add(KeyValue.pair(key, Helper.getFromObject(value.getOutputMessage().toString())));
            return result;
        });
        if (Boolean.TRUE.equals(DEBUG)) {
            outputStream.print(Printed.toSysOut());
        }
        outputStream.to(getOutputStreamName(), Produced.with(stringSerde, stringSerde));
    }

    /**
     * Drop empty analytics messages.
     */
    private void dropEmptyMessages() {
        //
    }

    /**
     * Run the operator logic.
     *
     * @param inputStream KStream<String, MessageModel>
     */
    private KStream<String, MessageModel> runOperatorLogic(KStream<String, MessageModel> inputStream) {
        return inputStream.flatMap((key, value) -> {
            List<KeyValue<String, MessageModel>> result = new LinkedList<>();
            operator.run(this.message.setMessage(value));
            result.add(KeyValue.pair(!Values.OPERATOR_ID.equals("debug") ? Values.OPERATOR_ID : Values.PIPELINE_ID, this.message.getMessage()));
            return result;
        });
    }

    /**
     * Filter the input stream as a record stream by operator ID or device ID.
     *
     * @param topic InputTopicModel
     * @param inputData KStream<String, T>
     * @return KStream<String, T>
     */
    private <T>KStream<String, T> filterStream(InputTopicModel topic, KStream<String, T> inputData) {
        KStream<String, T> filterData;
        String[] filterValues  = topic.getFilterValue().split(",");
        filterData = streamBuilder.filterBy(inputData, filterValues);
        return filterData;
    }

    private KStream<String, MessageModel> parseInputStream (InputTopicModel topicConfig){
        KStream<String, MessageModel> messagesStream;
        if (topicConfig.getName().split(",")[0].equals("analytics")){
            KStream<String, AnalyticsMessageModel> inputData = streamBuilder.getBuilder().stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.AnalyticsMessage()));
            //Filter Stream
            KStream<String, AnalyticsMessageModel> filteredStream = filterStream(topicConfig, inputData);
            messagesStream = filteredStream.flatMap((key, value) -> {
                MessageModel message = new MessageModel();
                message.putMessage(topicConfig.getName(), value);
                List<KeyValue<String, MessageModel>> result = new LinkedList<>();
                result.add(KeyValue.pair(key, message));
                return result;
            });
        } else {
            KStream<String, DeviceMessageModel> inputData = streamBuilder.getBuilder().stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.DeviceMessage()));
            //Filter Stream
            KStream<String, DeviceMessageModel> filteredStream = filterStream(topicConfig, inputData);
            messagesStream = inputData.flatMap((key, value) -> {
                MessageModel message = new MessageModel();
                message.putMessage(topicConfig.getName(), value);
                List<KeyValue<String, MessageModel>> result = new LinkedList<>();
                result.add(KeyValue.pair(key, message));
                return result;
            });
        }
        return messagesStream;
    }
}
