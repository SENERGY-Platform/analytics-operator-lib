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

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.cluster.Broker;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.InputMessageModel;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Helper {

    private static final String ZOOKEEPER_METRIC_GROUP = "zookeeper-metrics-group";
    private static final String ZOOKEEPER_METRIC_TYPE = "zookeeper";
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(Helper.class.getName());

    private Helper() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Returns a String list of kafka instances from zookeeper.
     *
     * @param zookeeperConnect zookeeper connection string
     * @return string list of kafka instances.
     */
    public static String getBrokerList(String zookeeperConnect) {
        if (zookeeperConnect == null || zookeeperConnect.equals("")) {
            return "localhost:2181";
        }
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZooKeeperClient zooKeeperClient = new ZooKeeperClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, 1, Time.SYSTEM, ZOOKEEPER_METRIC_GROUP, ZOOKEEPER_METRIC_TYPE);
        KafkaZkClient client = new KafkaZkClient(zooKeeperClient, false, Time.SYSTEM);
        client.getAllBrokersInCluster();
        List<String> brokerList = new ArrayList<>();
        List<Broker> brokers = JavaConversions.seqAsJavaList(client.getAllBrokersInCluster());
        for (Broker broker : brokers) {
            //assuming you do not enable security
            if (broker != null) {
                brokerList.add(broker.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
                        .host() + ":" + broker.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).port());
            }
        }
        client.close();
        zooKeeperClient.close();
        return String.join(",", brokerList);
    }

    /**
     * Returns the value of an env var or ,if the env var is not set, the default value.
     *
     * @param envName      the name of the env var
     * @param defaultValue the default return value
     * @return the value of the env var or the default value
     */
    public static String getEnv(String envName, String defaultValue) {
        if (defaultValue.equals("")) {
            defaultValue = null;
        }
        return System.getenv(envName) != null ? System.getenv(envName) : defaultValue;
    }

    public static Integer getEnv(String envName, Integer defaultValue) {
        return System.getenv(envName) != null ? Integer.parseInt(System.getenv(envName)) : defaultValue;
    }

    public static <T> String getFromObject(T object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    public static InputMessageModel analyticsToInputMessageModel(AnalyticsMessageModel input, String topicName) {
        InputMessageModel message = new InputMessageModel();
        message.setTopic(topicName);
        message.setFilterType(InputMessageModel.FilterType.OPERATOR_ID);
        message.setValue(input.getAnalytics());
        message.setFilterIdFirst(input.getOperatorId());
        message.setFilterIdSecond(input.getPipelineId());
        return message;
    }

    public static InputMessageModel deviceToInputMessageModel(DeviceMessageModel input, String topicName) {
        InputMessageModel message = new InputMessageModel();
        message.setTopic(topicName);
        message.setFilterType(InputMessageModel.FilterType.DEVICE_ID);
        message.setValue(input.getValue());
        message.setFilterIdFirst(input.getDeviceId());
        message.setFilterIdSecond(input.getServiceId());
        return message;
    }
}
