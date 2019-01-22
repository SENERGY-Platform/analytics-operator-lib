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

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.json.JSONObject;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;

public class Helper {

    /**
     * Returns a String list of kafka instances from zookeeper.
     *
     * @param zookeeperConnect zookeeper connection string
     * @return string list of kafka instances.
     */
    public static String getBrokerList(String zookeeperConnect){
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;

        ZkUtils zk = ZkUtils.apply(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, true);
        List<String> brokerList = new ArrayList<String>();
        List<Broker> brokers = JavaConversions.seqAsJavaList(zk.getAllBrokersInCluster());
        for (Broker broker : brokers) {
            //assuming you do not enable security
            if (broker != null) {
                brokerList.add(broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
                        .host()+":"+broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).port());
            }
        }
        zk.close();
        return String.join(",", brokerList);
    }

    /**
     * Returns the value of an env var or ,if the env var is not set, the default value.
     *
     * @param envName the name of the env var
     * @param defaultValue the default return value
     * @return the value of the env var or the default value
     */
    public static String getEnv(String envName, String defaultValue){
        if (defaultValue.equals("")){
            defaultValue = null;
        }
        return System.getenv(envName) != null ? System.getenv(envName) : defaultValue;
    }

    public static Integer getEnv(String envName, Integer defaultValue){
        return System.getenv(envName) != null ? Integer.parseInt(System.getenv(envName)) : defaultValue;
    }

    /**
     * Returns the value of the path of a JSON string.
     *
     * @param json a json string
     * @param path the path to be read from
     * @return the value of the path
     */
    public static String getJSONPathValue(String json, String path){
        if (checkPathExists(json, path)){
            if (!(JsonPath.read(json, "$."+ path) instanceof String)){
                return JsonPath.read(json, "$."+ path).toString();
            }
            return JsonPath.read(json, "$."+ path);
        }
        return "";
    }

    /**
     * Sets the path of a JSOn string to a value. If the path does not exist, it will be created.
     *
     * @param json a JSON string
     * @param path the path to be set
     * @param value the value to be set
     * @return the json string which was written
     */
    public static String setJSONPathValue(String json, String path, Object value){
        try {
            JsonPath.parse(json).read("$." + path);
        } catch (PathNotFoundException e) {
            String ps = "$";
            for(String p: path.split("\\.")){
                if (!checkPathExists(json, ps + "." + p)){
                    json = JsonPath.parse(json).put(ps, p, new JSONObject()).jsonString();
                }
                ps = ps + "." + p;
            }
        }
        return JsonPath.parse(json).set("$." + path, value).jsonString();
    }

    /**
     * Returns true, if a path exists in a JSON string. Otherwise false.
     *
     * @param json a JSON string
     * @param path the path to be tested
     * @return the result of the check
     */
    public static boolean checkPathExists (String json, String path){
        try {
            JsonPath.parse(json).read(path);
            return true;
        } catch (PathNotFoundException e) {
            //System.out.println("Path does not exist.");
        }
        return false;
    }
}
