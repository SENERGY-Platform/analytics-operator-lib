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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.StreamsConfigProvider;
import org.json.simple.JSONArray;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;


@Ignore
public class StreamIntegrationTest {

    private static final String inputTopic = "input-stream";
    private static final String outputTopic = "output-stream";

    @After
    public void setStreamConfigString() {
        StreamsConfigProvider.setZookeeperConnectionString("");
    }

    @Test
    public void testStreamStart() throws Exception {
        JSONArray messages = new JSONHelper().parseFile("stream/testStreamStartMessages.json");
        List<String> inputValues = Arrays.asList(
                messages.get(0).toString(),
                messages.get(1).toString(),
                messages.get(2).toString()
        );
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("stream/testStreamStartConfig.json").toString()));
        KafkaContainer kafka = new KafkaContainer();
        kafka.start();

        System.out.println(kafka.getBootstrapServers());
        StreamsConfigProvider.setKafkaBootstrapString(kafka.getBootstrapServers());
        Stream stream = new Stream();
        TestOperator operator = new TestOperator();
        stream.start(operator);

        //Setup producer
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig, Time.SYSTEM);
        List<KeyValue<String, String>> results = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
                outputTopic, 2);
        stream.closeStreams();
        //stop kafka
        kafka.stop();
    }
}
