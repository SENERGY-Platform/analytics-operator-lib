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

package org.infai.ses.senergy.testing.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JSONHelper {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(JSONHelper.class.getName());
    private static final String PATH_PREFIX = "src/test/resources/";

    public <K> K parseFile(String fileName) {
        JSONParser parser = new JSONParser();
        ClassLoader classLoader = getClass().getClassLoader();
        try {
            return (K) parser.parse(new FileReader(classLoader.getResource(fileName).getFile()));

        } catch (Exception e) {
            log.log(Level.SEVERE,e.getMessage());
        }
        return null;
    }

    /**
     * Returns a value of a json string.
     *
     * @param key
     * @param <K>
     * @return value of json string
     */
    public static  <K> K getValue (String key, String jsonMessage){
        JSONParser parser = new JSONParser();
        try {
            JSONObject obj = (JSONObject) parser.parse(jsonMessage);
            return (K) obj.get(key);
        } catch (Exception e) {
            log.log(Level.SEVERE,e.getMessage());
        }
        return null;
    }

    public static <T> T getObjectFromJSONString(String jsonString, Class<T> tClass){
        try {
            return objectMapper.readValue(jsonString, tClass);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T> T getObjectFromJSONPath(String Path, Class<T> tClass){
        try {
            return objectMapper.readValue(new File(PATH_PREFIX+Path), tClass);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public <T> List<T> getObjectArrayFromJSONPath(String Path){
        try {
            return objectMapper.readValue(new File(PATH_PREFIX+Path), new TypeReference<>() {
            });
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
