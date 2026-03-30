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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.infai.ses.senergy.models.*;
import org.infai.ses.senergy.serialization.JSONSerdes;
import org.infai.ses.senergy.utils.ApplicationState;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.StreamsConfigProvider;
import org.infai.ses.senergy.utils.TimeProvider;

import java.util.*;
import java.util.function.Function;

public class Stream {

    private KStream<String, String> outputStream;
    private static final Boolean DEBUG = Boolean.valueOf(Helper.getEnv("DEBUG", "false"));
    private final Boolean resetApp = Boolean.valueOf(Helper.getEnv("RESET_APP", "false"));
    private final Boolean kTableProcessing = Boolean.valueOf(Helper.getEnv("KTABLE_PROCESSING", "true"));
    private OperatorInterface operator;
    private Message message = new Message();
    private final Config config = ConfigProvider.getConfig();
    private KafkaStreams streams;
    private final StreamsBuilder builder = new StreamsBuilder();
    private Integer windowTime = Values.WINDOW_TIME;
    private final String originalInputName = "original_input_ids";

    /**
     * Start the streams application.
     *
     * @param runOperator OperatorInterface
     */
    public void start(OperatorInterface runOperator) {
        operator = runOperator;
        message = operator.configMessage(message);
        message.addFlexInput(originalInputName);
        if (config.topicCount() > 1) {
            if (kTableProcessing) {
                processMultipleStreamsAsTable(config.getInputTopicsConfigs());
            } else {
                processMultipleStreams(config.getInputTopicsConfigs());
            }
        } else if (config.topicCount() == 1) {
            processSingleStream(config.getInputTopicsConfigs().get(0));
        }
        streams = new KafkaStreams(this.builder.build(), StreamsConfigProvider.getStreamsConfiguration());
        if (resetApp) {
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

        if (DEBUG) {
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
        if (DEBUG) {
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
        if (DEBUG) {
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
        List<KTable<String, InputMessageModel>> inputTables = new ArrayList<>();
        for (KStream<String, InputMessageModel> inputStream : inputStreams) {
            if (DEBUG) {
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
        if (DEBUG) {
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
        assert merged != null;
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
        if (DEBUG) {
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
            Message localMessage = new Message();
            localMessage = operator.configMessage(localMessage);

            value.getOutputMessage().setTime(TimeProvider.nowUTCToString());
            localMessage.setMessage(value);

            operator.run(localMessage);

            MessageModel resultMessage = localMessage.getMessage();
            if (!resultMessage.getOutputMessage().getAnalytics().isEmpty()) {
                setInputID(localMessage);
                String outKey = !Values.OPERATOR_ID.equals("debug")
                        ? Values.OPERATOR_ID
                        : Values.PIPELINE_ID;
                return List.of(KeyValue.pair(outKey, resultMessage));
            }
            return List.of();
        });
    }

    /**
     * Set the input id (device, import, operator) to the output message.
     *
     * @param message Message
     */
    private void setInputID(Message message) {
        FlexInput original_input_id = message.getFlexInput(originalInputName);
        String original_input_id_str = "";
        boolean operatorHasOriginalInput = false;
        try {
            original_input_id_str = original_input_id.getString();
        } catch (Exception e) {
            operatorHasOriginalInput = true;
        }

        if(operatorHasOriginalInput) {
            List<String> originalInputs = new ArrayList<>();
            Map<String, InputMessageModel> inputMessages = message.getMessage().getMessages();
            for (String s : inputMessages.keySet()) {
                InputMessageModel inputMessage = inputMessages.get(s);
                String filterValue = inputMessage.getFilterIdFirst();
                originalInputs.add(filterValue);
            }

            message.output(originalInputName, String.join(",", originalInputs));
        } else {
            message.output(originalInputName, original_input_id_str);
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
        String[] filterValues1 = topic.getFilterValue().split(",");
        String[] filterValues2 = Optional.ofNullable(topic.getFilterValue2())
                .map(v -> v.split(","))
                .orElse(new String[]{Values.PIPELINE_ID});
        return StreamBuilder.filterBy(inputData, filterValues1, filterValues2);
    }


    /**
     * Parse the input stream as a record stream.
     *
     * @param topicConfig InputTopicModel
     * @param streamLineKey Boolean
     * @return KStream<String, InputMessageModel>
     */
    private KStream<String, InputMessageModel> parseInputStream(InputTopicModel topicConfig, Boolean streamLineKey) {
        return switch (topicConfig.getFilterType()) {
            case "OperatorId" ->
                    convertTopicStream(topicConfig, builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.AnalyticsMessage())),
                            streamLineKey, v -> Helper.analyticsToInputMessageModel(v, topicConfig.getName()));
            case "ImportId" ->
                    convertTopicStream(topicConfig, builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.ImportMessage())),
                            streamLineKey, v -> Helper.importToInputMessageModel(v, topicConfig.getName()));
            default ->
                    convertTopicStream(topicConfig, builder.stream(topicConfig.getName(), Consumed.with(Serdes.String(), JSONSerdes.DeviceMessage())),
                            streamLineKey, v -> Helper.deviceToInputMessageModel(v, topicConfig.getName()));
        };
    }

    private <T> KStream<String, InputMessageModel> convertTopicStream(
            InputTopicModel topicConfig, KStream<String, T> inputStream, Boolean streamLineKey, Function<T, InputMessageModel> converter
    ) {
        KStream<String, T> filteredStream = filterStream(topicConfig, inputStream);
        checkApplicationStatus();
        return convertToInputStream(streamLineKey, filteredStream, converter);
    }


    /**
     * Parse the input streams as a record stream.
     *
     * @param topicConfigs List<InputTopicModel>
     * @param streamLineKey Boolean
     * @return List<KStream<String, InputMessageModel>>
     */
    private List<KStream<String, InputMessageModel>> parseStreams(List<InputTopicModel> topicConfigs, Boolean streamLineKey) {
        List<KStream<String, InputMessageModel>> inputStreams = new ArrayList<>();
        Map<String, KStream<?, ?>> inputMap = new HashMap<>(); // Unified map for all types

        for (InputTopicModel topicConfig : topicConfigs) {
            KStream<String, InputMessageModel> parsedInputStream = switch (topicConfig.getFilterType()) {
                case "OperatorId" -> getOrCreateStream(topicConfig, streamLineKey, inputMap,
                        JSONSerdes.AnalyticsMessage(), v -> Helper.analyticsToInputMessageModel(v, topicConfig.getName()));
                case "ImportId" -> getOrCreateStream(topicConfig, streamLineKey, inputMap,
                        JSONSerdes.ImportMessage(), v -> Helper.importToInputMessageModel(v, topicConfig.getName()));
                default -> // DeviceId
                        getOrCreateStream(topicConfig, streamLineKey, inputMap,
                                JSONSerdes.DeviceMessage(), v -> Helper.deviceToInputMessageModel(v, topicConfig.getName()));
            };
            inputStreams.add(parsedInputStream);
        }

        return inputStreams;
    }

    /**
     * Generic helper to get or create a KStream, filter it, process offsets, and convert to InputMessageModel.
     */
    private <T> KStream<String, InputMessageModel> getOrCreateStream(
            InputTopicModel topicConfig,
            Boolean streamLineKey,
            Map<String, KStream<?, ?>> inputMap,
            Serde<T> serde,
            Function<T, InputMessageModel> converter
    ) {
        @SuppressWarnings("unchecked")
        KStream<String, T> inputData = (KStream<String, T>) inputMap.computeIfAbsent(topicConfig.getName(), name -> {
            KStream<String, T> stream = builder.stream(name, Consumed.with(Serdes.String(), serde));
            checkApplicationStatus();
            return stream;
        });

        KStream<String, T> filteredStream = filterStream(topicConfig, inputData);
        return convertToInputStream(streamLineKey, filteredStream, converter);
    }

    private <T> KStream<String, InputMessageModel> convertToInputStream(
            Boolean streamLineKey, KStream<String, T> filteredStream, Function<T, InputMessageModel> converter
    ) {
        return filteredStream.flatMap((key, value) -> {
            String outKey = Boolean.TRUE.equals(streamLineKey) ? "A" : key;
            return List.of(KeyValue.pair(outKey, converter.apply(value)));
        });
    }

    /**
     * Converts an InputMessageModel stream to a MessageModel stream.
     *
     * @param stream The stream to be converted.
     * @return The converted stream.
     */
    private KStream<String, MessageModel> toMessageModel(KStream<String, InputMessageModel> stream) {
        return stream.map((key, value) -> {
            MessageModel messageModel = new MessageModel();
            messageModel.putMessage(value.getTopic(), value);
            return KeyValue.pair(key, messageModel);
        });
    }

    /**
     * Checks the application status and closes the streams if the error status is not 0.
     */
    private void checkApplicationStatus(){
        if (ApplicationState.getErrorStatus() != 0){
            this.streams.close();
        }
    }
}
