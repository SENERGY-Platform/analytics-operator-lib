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

package org.infai.seits.sepl.operators.test;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;

public class JSONFileReader {

    public <K> K parseFile(String fileName) {
        JSONParser parser = new JSONParser();
        ClassLoader classLoader = getClass().getClassLoader();
        try {
            return (K) parser.parse(new FileReader(classLoader.getResource(fileName).getFile()));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
