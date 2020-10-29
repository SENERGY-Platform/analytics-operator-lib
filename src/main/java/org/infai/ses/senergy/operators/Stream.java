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
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.StreamsConfigProvider;

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
        KTable<String, InputMessageModel> messagesStream = parseInputStreamAsTable(topicConfig);
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
        List<KStream<String, InputMessageModel>> inputStreams = new LinkedList<>();
        for (InputTopicModel topicConfig : topicConfigs) {
            KStream<String, InputMessageModel> filteredInputStream = parseInputStream(topicConfig, true);
            inputStreams.add(filteredInputStream);
            if (Boolean.TRUE.equals(DEBUG)) {
                filteredInputStream.print(Printed.toSysOut());
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
        List<KTable<String, InputMessageModel>> inputStreams = new LinkedList<>();
        for (InputTopicModel topicConfig : topicConfigs) {
            KTable<String, InputMessageModel> filteredInputStream = parseInputStream(topicConfig, true).toTable(Materialized.with(Serdes.String(), JSONSerdes.InputMessage()));
            inputStreams.add(filteredInputStream);
            if (Boolean.TRUE.equals(DEBUG)) {
                filteredInputStream.toStream().print(Printed.toSysOut());
            }
        }

        KStream<String, MessageModel> afterOperatorStream = runOperatorLogic(TableBuilder.joinMultipleStreams(inputStreams).toStream());
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
            operator.run(this.message.setMessage(value));
            result.add(KeyValue.pair(!Values.OPERATOR_ID.equals("debug") ? Values.OPERATOR_ID : Values.PIPELINE_ID, this.message.getMessage()));
            return result;
        });
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
        filterData = StreamBuilder.filterBy(inputData, filterValues);
        return filterData;
    }

    /**
     * Filter the input stream as a changelog stream by operator ID or device ID.
     *
     * @param topic     InputTopicModel
     * @param inputData KStream<String, T>
     * @return KStream<String, T>
     */
    private <T> KTable<String, T> filterStream(InputTopicModel topic, KTable<String, T> inputData) {
        KTable<String, T> filterData;
        String[] filterValues = topic.getFilterValue().split(",");
        filterData = TableBuilder.filterBy(inputData, filterValues);
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
            return filteredStream.flatMap((key, value) -> {
                List<KeyValue<String, InputMessageModel>> result = new LinkedList<>();
                result.add(KeyValue.pair(Boolean.TRUE.equals(streamLineKey) ? "A" : key, Helper.analyticsToInputMessageModel(value, topicConfig.getName())));
                return result;
            });
        } else {
            KStream<String, DeviceMessageModel> inputData = this.builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.DeviceMessage()));
            KStream<String, DeviceMessageModel> filteredStream = filterStream(topicConfig, inputData);
            return filteredStream.flatMap((key, value) -> {
                List<KeyValue<String, InputMessageModel>> result = new LinkedList<>();
                result.add(KeyValue.pair(Boolean.TRUE.equals(streamLineKey) ? "A" : key, Helper.deviceToInputMessageModel(value, topicConfig.getName())));
                return result;
            });
        }
    }

    /**
     * Get a changelog stream by topic config.
     *
     * @param topicConfig InputTopicModel
     * @return KStream<String, InputMessageModel>
     */
    private KTable<String, InputMessageModel> parseInputStreamAsTable(InputTopicModel topicConfig) {
        if (topicConfig.getFilterType().equals("OperatorId")) {
            KTable<String, AnalyticsMessageModel> inputData = this.builder.table(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.AnalyticsMessage()));
            KTable<String, AnalyticsMessageModel> filteredStream = filterStream(topicConfig, inputData);
            return filteredStream.mapValues(value -> Helper.analyticsToInputMessageModel(value, topicConfig.getName()));
        } else {
            KTable<String, DeviceMessageModel> inputData = this.builder.table(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.DeviceMessage()));
            KTable<String, DeviceMessageModel> filteredStream = filterStream(topicConfig, inputData);
            return filteredStream.mapValues(value -> Helper.deviceToInputMessageModel(value, topicConfig.getName()));
        }
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
}
