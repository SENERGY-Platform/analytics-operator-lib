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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.infai.ses.senergy.models.*;
import org.infai.ses.senergy.serialization.JSONSerdes;
import org.infai.ses.senergy.utils.ApplicationState;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.StreamsConfigProvider;
import org.infai.ses.senergy.utils.TimeProvider;

import java.util.*;

public class Stream {

    private KStream<String, String> outputStream;
    private static final Boolean DEBUG = Boolean.valueOf(Helper.getEnv("DEBUG", "false"));
    private final Boolean resetApp = Boolean.valueOf(Helper.getEnv("RESET_APP", "false"));
    private final Boolean kTableProcessing = Boolean.valueOf(Helper.getEnv("KTABLE_PROCESSING", "true"));
    private OperatorInterface operator;
    private Message message = new Message();
    private final Config config = ConfigProvider.getConfig();
    private KafkaStreams streams;
    private StreamsBuilder builder = new StreamsBuilder();
    private Integer windowTime = Values.WINDOW_TIME;

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
                processMultipleStreamsAsTable(config.getInputTopicsConfigs());
            } else {
                processMultipleStreams(config.getInputTopicsConfigs());
            }
        } else if (config.topicCount() == 1) {
            processSingleStream(config.getInputTopicsConfigs().get(0));
        }
        streams = new KafkaStreams(this.builder.build(), StreamsConfigProvider.getStreamsConfiguration());
        if (Boolean.TRUE.equals(resetApp)) {
            streams.cleanUp();
        }

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Close the streams application.
     */
    public void closeStreams() {
        streams.close();
    }

    /**
     * Process a single stream as a record stream.
     *
     * @param topicConfig InputTopicModel
     */
    public void processSingleStream(InputTopicModel topicConfig) {
        KStream<String, InputMessageModel> messagesStream = parseInputStream(topicConfig, false);

        if (Boolean.TRUE.equals(DEBUG)) {
            messagesStream.print(Printed.toSysOut());
        }
        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(toMessageModel(messagesStream));
        outputStream(afterOperatorStream);
    }

    /**
     * Process a single stream as a changelog stream.
     *
     * @param topicConfig InputTopicModel
     */
    public void processSingleStreamAsTable(InputTopicModel topicConfig) {
        KTable<String, InputMessageModel> messagesStream = parseInputStream(topicConfig, false).toTable(Materialized.with(Serdes.String(), JSONSerdes.InputMessage()));
        if (Boolean.TRUE.equals(DEBUG)) {
            messagesStream.toStream().print(Printed.toSysOut());
        }
        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(toMessageModel(messagesStream.toStream()));
        outputStream(afterOperatorStream);
    }

    /**
     * Processes multiple streams as a record stream while automatically creating only one inputStream per topic.
     *
     * @param topicConfigs List<InputTopicModel>
     */
    public void processMultipleStreams(List<InputTopicModel> topicConfigs) {
        List<KStream<String, InputMessageModel>> inputStreams = parseStreams(topicConfigs, true);
        if (Boolean.TRUE.equals(DEBUG)) {
            for (KStream<String, InputMessageModel> inputStream : inputStreams) {
                inputStream.print(Printed.toSysOut());
            }
        }
        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(StreamBuilder.joinMultipleStreams(inputStreams, windowTime));
        outputStream(afterOperatorStream);
    }

    /**
     * Processes multiple streams as a changelog stream while automatically creating only one inputStream per topic.
     *
     * @param topicConfigs List<InputTopicModel>
     */
    public void processMultipleStreamsAsTable(List<InputTopicModel> topicConfigs) {
        List<KStream<String, InputMessageModel>> inputStreams = parseStreams(topicConfigs, true);
        List<KTable<String, InputMessageModel>> inputTables = new LinkedList<>();
        for (KStream<String, InputMessageModel> inputStream : inputStreams) {
            if (Boolean.TRUE.equals(DEBUG)) {
                inputStream.print(Printed.toSysOut());
            }
            inputTables.add(inputStream.toTable(Materialized.with(Serdes.String(), JSONSerdes.InputMessage())));
        }
        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(TableBuilder.joinMultipleStreams(inputTables).toStream());
        outputStream(afterOperatorStream);
    }

    /**
     * Merge multiple streams and process them as one.
     *
     * @param topicConfigs List<InputTopicModel>
     */
    public void mergeMultipleStreams(List<InputTopicModel> topicConfigs){
        List<KStream<String, InputMessageModel>> inputStreams = parseStreams(topicConfigs, true);
        if (Boolean.TRUE.equals(DEBUG)) {
            for (KStream<String, InputMessageModel> inputStream : inputStreams) {
                inputStream.print(Printed.toSysOut());
            }
        }
        KStream<String, InputMessageModel> merged = null;
        for (KStream<String, InputMessageModel> inputStream : inputStreams) {
            if (merged != null){
                merged = merged.merge(inputStream);
            } else {
                merged = inputStream;
            }
        }
        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(toMessageModel(merged));
        outputStream(afterOperatorStream);
    }

    /**
     * Returns the output stream.
     *
     * @return KStream<String, String>
     */
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
    public void setOperator(OperatorInterface runOperator) {
        operator = runOperator;
    }

    /**
     * Get the stream builder.
     *
     * @return StreamsBuilder
     */
    public StreamsBuilder getBuilder() {
        return builder;
    }

    /**
     * Set the stream window time.
     *
     * @param windowTime Integer
     */
    public void setWindowTime(Integer windowTime) {
        this.windowTime = windowTime;
    }

    /**
     * Output the stream.
     */
    private void outputStream(KStream<String, MessageModel> inputStream) {
        outputStream = inputStream.mapValues(value -> Helper.getFromObject(value.getOutputMessage()));
        if (Boolean.TRUE.equals(DEBUG)) {
            outputStream.print(Printed.toSysOut());
        }
        outputStream.to(getOutputStreamName(), Produced.with(Serdes.String(), Serdes.String()));
    }

    /**
     * Run the operator logic.
     *
     * @param inputStream KStream<String, MessageModel>
     */
    private KStream<String, MessageModel> runOperatorLogic(KStream<String, MessageModel> inputStream) {
        return inputStream.flatMap((key, value) -> {
            List<KeyValue<String, MessageModel>> result = new LinkedList<>();
            value.getOutputMessage().setTime(TimeProvider.nowUTCToString());
            this.message.setMessage(value);
            operator.run(this.message);
            setInputID(this.message);
            MessageModel message = this.message.getMessage();
            if (message.getOutputMessage().getAnalytics().size() > 0) {
                result.add(KeyValue.pair(!Values.OPERATOR_ID.equals("debug") ? Values.OPERATOR_ID : Values.PIPELINE_ID, message));
            }
            return result;
        });
    }

    /**
     * Set the input id (device, import, operator) to the output message.
     *
     * @param message Message
     */
    private void setInputID(Message message) {
        Map<String, InputMessageModel> inputMessages = message.getMessage().getMessages();
        // TODO: what should happen with multiple input messages?
        InputMessageModel inputMessage = inputMessages.get(inputMessages.keySet().iterator().next());
        InputMessageModel.FilterType filterType = inputMessage.getFilterType();
        String filterValue = inputMessage.getFilterIdFirst();
        switch (filterType) {
            case OPERATOR_ID:
                message.output("operator_id", filterValue);
                break;

            case DEVICE_ID:
                message.output("device_id", filterValue);
                break;
        
            case IMPORT_ID:
                message.output("import_id", filterValue);
                break;
        
            default:
                break;
        }
    }

    /**
     * Filter the input stream as a record stream by operator ID or device ID.
     *
     * @param topic     InputTopicModel
     * @param inputData KStream<String, T>
     * @return KStream<String, T>
     */
    private <T> KStream<String, T> filterStream(InputTopicModel topic, KStream<String, T> inputData) {
        KStream<String, T> filterData;
        String[] filterValues = topic.getFilterValue().split(",");
        String[] filterValues2;
        if (topic.getFilterValue2() != null){
            filterValues2 = topic.getFilterValue2().split(",");
        } else {
            filterValues2 = new String[]{Values.PIPELINE_ID};
        }
        filterData = StreamBuilder.filterBy(inputData, filterValues, filterValues2);
        return filterData;
    }

    /**
     * Get a record stream by topic config.
     *
     * @param topicConfig InputTopicModel
     * @return KStream<String, InputMessageModel>
     */
    private KStream<String, InputMessageModel> parseInputStream(InputTopicModel topicConfig, Boolean streamLineKey) {
        if (topicConfig.getFilterType().equals("OperatorId")) {
            KStream<String, AnalyticsMessageModel> inputData = this.builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.AnalyticsMessage()));
            KStream<String, AnalyticsMessageModel> filteredStream = filterStream(topicConfig, inputData);
            filteredStream.process(OffsetCheck::new);
            this.checkApplicationStatus();
            return analyticsStreamToInputStream(streamLineKey, topicConfig,filteredStream);
        } else if (topicConfig.getFilterType().equals("ImportId")) {
            KStream<String, ImportMessageModel> inputData = this.builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.ImportMessage()));
            KStream<String, ImportMessageModel> filteredStream = filterStream(topicConfig, inputData);
            filteredStream.process(OffsetCheck::new);
            this.checkApplicationStatus();
            return importStreamToInputStream(streamLineKey, topicConfig,filteredStream);
        } else {
            KStream<String, DeviceMessageModel> inputData = this.builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.DeviceMessage()));
            KStream<String, DeviceMessageModel> filteredStream = filterStream(topicConfig, inputData);
            filteredStream.process(OffsetCheck::new);
            this.checkApplicationStatus();
            return deviceStreamToInputStream(streamLineKey, topicConfig, filteredStream);
        }
    }

    private List<KStream<String, InputMessageModel>> parseStreams (List<InputTopicModel> topicConfigs, Boolean streamLineKey) {
        List<KStream<String, InputMessageModel>> inputStreams = new LinkedList<>();
        Map<String, KStream<String, AnalyticsMessageModel>> analyticsInputMap = new HashMap<>();
        Map<String, KStream<String, DeviceMessageModel>> devicesInputMap = new HashMap<>();
        Map<String, KStream<String, ImportMessageModel>> importInputMap = new HashMap<>();
        for (InputTopicModel topicConfig : topicConfigs) {
            KStream<String, InputMessageModel> parsedInputStream;
            if (topicConfig.getFilterType().equals("OperatorId")) {
                KStream<String, AnalyticsMessageModel> inputData;
                if (!analyticsInputMap.containsKey(topicConfig.getName())) {
                    inputData = this.builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.AnalyticsMessage()));
                    inputData.process(OffsetCheck::new);
                    this.checkApplicationStatus();
                    analyticsInputMap.put(topicConfig.getName(), inputData);
                } else {
                    inputData = analyticsInputMap.get(topicConfig.getName()).branch(
                            (key, value) -> true
                    )[0];
                }
                KStream<String, AnalyticsMessageModel> filteredStream = filterStream(topicConfig, inputData);
                parsedInputStream = analyticsStreamToInputStream(streamLineKey, topicConfig, filteredStream);
            } else if (topicConfig.getFilterType().equals("ImportId")) {
                KStream<String, ImportMessageModel> inputData;
                if (!analyticsInputMap.containsKey(topicConfig.getName())) {
                    inputData = this.builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.ImportMessage()));
                    inputData.process(OffsetCheck::new);
                    this.checkApplicationStatus();
                    importInputMap.put(topicConfig.getName(), inputData);
                } else {
                    inputData = importInputMap.get(topicConfig.getName()).branch(
                            (key, value) -> true
                    )[0];
                }
                KStream<String, ImportMessageModel> filteredStream = filterStream(topicConfig, inputData);
                parsedInputStream = importStreamToInputStream(streamLineKey, topicConfig, filteredStream);
            } else {
                KStream<String, DeviceMessageModel> inputData;
                if (!devicesInputMap.containsKey(topicConfig.getName())) {
                    inputData = this.builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.DeviceMessage()));
                    inputData.process(OffsetCheck::new);
                    this.checkApplicationStatus();
                    devicesInputMap.put(topicConfig.getName(), inputData);
                } else {
                    inputData = devicesInputMap.get(topicConfig.getName()).branch(
                            (key, value) -> true
                    )[0];
                }
                KStream<String, DeviceMessageModel> filteredStream = filterStream(topicConfig, inputData);
                parsedInputStream = deviceStreamToInputStream(streamLineKey, topicConfig, filteredStream);
            }
            inputStreams.add(parsedInputStream);
        }
        return inputStreams;
    }

    private KStream<String, InputMessageModel> deviceStreamToInputStream(Boolean streamLineKey, InputTopicModel topicConfig, KStream<String, DeviceMessageModel> filteredStream) {
        KStream<String, InputMessageModel> parsedInputStream;
        parsedInputStream =  filteredStream.flatMap((key, value) -> {
            List<KeyValue<String, InputMessageModel>> result = new LinkedList<>();
            result.add(KeyValue.pair(Boolean.TRUE.equals(streamLineKey) ? "A" : key, Helper.deviceToInputMessageModel(value, topicConfig.getName())));
            return result;
        });
        return parsedInputStream;
    }

    private KStream<String, InputMessageModel> analyticsStreamToInputStream(Boolean streamLineKey, InputTopicModel topicConfig, KStream<String, AnalyticsMessageModel> filteredStream) {
        KStream<String, InputMessageModel> parsedInputStream;
        parsedInputStream =  filteredStream.flatMap((key, value) -> {
            List<KeyValue<String, InputMessageModel>> result = new LinkedList<>();
            result.add(KeyValue.pair(Boolean.TRUE.equals(streamLineKey) ? "A" : key, Helper.analyticsToInputMessageModel(value, topicConfig.getName())));
            return result;
        });
        return parsedInputStream;
    }

    private KStream<String, InputMessageModel> importStreamToInputStream(Boolean streamLineKey, InputTopicModel topicConfig, KStream<String, ImportMessageModel> filteredStream) {
        KStream<String, InputMessageModel> parsedInputStream;
        parsedInputStream =  filteredStream.flatMap((key, value) -> {
            List<KeyValue<String, InputMessageModel>> result = new LinkedList<>();
            result.add(KeyValue.pair(Boolean.TRUE.equals(streamLineKey) ? "A" : key, Helper.importToInputMessageModel(value, topicConfig.getName())));
            return result;
        });
        return parsedInputStream;
    }

    private KStream<String, MessageModel> toMessageModel(KStream<String, InputMessageModel> stream) {
        return stream.flatMap((key, value) -> {
            MessageModel messageModel = new MessageModel();
            messageModel.putMessage(value.getTopic(), value);
            List<KeyValue<String, MessageModel>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, messageModel));
            return result;
        });
    }

    private void checkApplicationStatus(){
        if (ApplicationState.getErrorStatus() != 0){
            this.streams.close();
        }
    }
}
