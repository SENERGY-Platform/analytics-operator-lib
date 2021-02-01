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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
            log.log(Level.SEVERE, e.getMessage());
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
    public static <K> K getValue(String key, String jsonMessage) {
        JSONParser parser = new JSONParser();
        try {
            JSONObject obj = (JSONObject) parser.parse(jsonMessage);
            return (K) obj.get(key);
        } catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage());
        }
        return null;
    }

    public static <T> T getObjectFromJSONString(String jsonString, Class<T> tClass) {
        try {
            return objectMapper.readValue(jsonString, tClass);
        } catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    public static <T> T getObjectFromJSONPath(String path, Class<T> tClass) {
        try {
            return objectMapper.readValue(new File(PATH_PREFIX + path), tClass);
        } catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    public static  <T> List<T> getObjectArrayFromJSONPath(String path, Class<T> tClass) {
        try {
            JsonNode tree = objectMapper.readTree(new File(PATH_PREFIX + path));
            ArrayList list = new ArrayList<T>();
            for (JsonNode node : tree){
                list.add(objectMapper.treeToValue(node, tClass));
            }
            return list;
        } catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage());
            return new LinkedList<>();
        }
    }

    /**
     * Returns the contents of a file as a String.
     *
     * @param filePath String
     * @return String
     */
    public static String readFileAsString(String  filePath) {
        try {
            return new String(Files.readAllBytes(Paths.get(PATH_PREFIX + filePath)));
        } catch (IOException e) {
            log.log(Level.SEVERE, e.getMessage());
            return "";
        }
    }

    /**
     * Returns the contents of a file as a byte array.
     *
     * @param filePath String
     * @return byte[]
     */
    public static byte[] readFileAsBytes(String filePath) {
        try {
            return new String(Files.readAllBytes(Paths.get(PATH_PREFIX + filePath))).getBytes();
        } catch (IOException e) {
            log.log(Level.SEVERE, e.getMessage());
            return new byte[0];
        }
    }
}
