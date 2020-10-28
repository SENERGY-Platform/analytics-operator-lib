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

import org.json.JSONObject;


public class HelperTest {

    protected JSONObject ob1 = new JSONObject();
    protected JSONObject ob2 = new JSONObject();
    protected JSONObject ob3 = new JSONObject();

    public HelperTest (){
        ob1.put("device_id", "1").put("value", new JSONObject().put("metrics", new JSONObject().put("level", Double.valueOf(5.4))));
        ob2.put("device_id", "1").put("value", new JSONObject().put("metrics", new JSONObject().put("level", Integer.valueOf(5))));
        ob3.put("device_id", "1").put("value", new JSONObject().put("metrics", new JSONObject().put("level", new String("{lat: 5.1213123, lon: 23.123123}"))));
    }
}
