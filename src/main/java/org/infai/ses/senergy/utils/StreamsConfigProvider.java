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

package org.infai.ses.senergy.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.infai.ses.senergy.operators.Helper;

import java.util.Properties;

public class StreamsConfigProvider {

    private static final Properties streamsConfiguration = new Properties();
    private static String zookeeperConnectionString = Helper.getEnv("ZK_QUORUM", "");
    private static String kafkaBootstrapString = null;

    private static void setProperties(){
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, Helper.getEnv("CONFIG_APPLICATION_ID", "stream-operator"));
        if (kafkaBootstrapString != null){
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapString);
        } else {
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Helper.getEnv("CONFIG_BOOTSTRAP_SERVERS", Helper.getBrokerList(zookeeperConnectionString)));
        }
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Helper.getEnv("STREAM_THREADS_CONFIG", "1"));
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Helper.getEnv("CONSUMER_AUTO_OFFSET_RESET_CONFIG", "earliest"));
    }

    public static Properties getStreamsConfiguration(){
        setProperties();
        return streamsConfiguration;
    }

    public static void setZookeeperConnectionString(String zkString){
        zookeeperConnectionString = zkString;
    }

    public static void setKafkaBootstrapString(String kafkaBString){
        kafkaBootstrapString = kafkaBString;
    }
}
