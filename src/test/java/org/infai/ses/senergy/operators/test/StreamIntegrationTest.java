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

package org.infai.ses.senergy.operators.test;

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.testing.utils.JSONFileReader;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.StreamsConfigProvider;
import org.json.simple.JSONArray;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class StreamIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;
    private static final String inputTopic = "input-stream";
    private static final String outputTopic = "output-stream";

    @After
    public void setStreamConfigString() {
        StreamsConfigProvider.setZookeeperConnectionString("");
    }

    @Test
    public void testStreamStart() throws Exception{
        ConfigProvider.setConfig(new Config(new JSONFileReader().parseFile("stream/testStreamStartConfig.json").toString()));
        JSONArray messages = new JSONFileReader().parseFile("stream/testStreamStartMessages.json");
        CLUSTER.createTopics(inputTopic, outputTopic);
        List<String> inputValues = Arrays.asList(
                messages.get(0).toString(),
                messages.get(1).toString(),
                messages.get(2).toString()
                );
        StreamsConfigProvider.setZookeeperConnectionString(CLUSTER.zKConnectString());
        IntegrationTestUtils.purgeLocalStreamsState(StreamsConfigProvider.getStreamsConfiguration());
        Stream stream = new Stream();
        TestOperator operator = new TestOperator();
        stream.start(operator);

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig, mockTime);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<KeyValue<String, String>> results = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
                outputTopic, 2);
        stream.closeStreams();
    }
}
