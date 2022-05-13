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

public final class Values {

    private Values() {
        throw new IllegalStateException("Utility class");
    }

    public static final String TOPIC_NAME_KEY = "name";
    public static final String MAPPINGS_KEY = "mappings";
    public static final String MAPPING_DEST_KEY = "dest";
    public static final String MAPPING_SOURCE_KEY = "source";
    public static final String FILTER_TYPE_KEY = "filterType";
    public static final String FILTER_VALUE_KEY = "filterValue";
    public static final String FILTER_VALUE_2_KEY = "filterValue2";
    public static final String FILTER_TYPE_OPERATOR_KEY = "OperatorId";
    public static final String FILTER_TYPE_DEVICE_KEY = "DeviceId";
    public static final String INPUT_TOPICS = "inputTopics";
    public static final Integer WINDOW_TIME = Helper.getEnv("WINDOW_TIME", 100);
    public static final String PIPELINE_ID = Helper.getEnv("PIPELINE_ID", "debug");
    public static final String OPERATOR_ID = Helper.getEnv("OPERATOR_ID", "debug");
    public static final String USER_ID = Helper.getEnv("USER_ID", "not set");
}
