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

package org.infai.seits.sepl.operators.test;

import com.jayway.jsonpath.JsonPath;
import org.infai.seits.sepl.operators.Helper;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class HelperTest {

    protected JSONObject ob1 = new JSONObject();
    protected JSONObject ob2 = new JSONObject();
    protected JSONObject ob3 = new JSONObject();

    public HelperTest (){
        ob1.put("device_id", "1").put("value", new JSONObject().put("metrics", new JSONObject().put("level", new Double(5.4))));
        ob2.put("device_id", "1").put("value", new JSONObject().put("metrics", new JSONObject().put("level", new Integer(5))));
        ob3.put("device_id", "1").put("value", new JSONObject().put("metrics", new JSONObject().put("level", new String("{lat: 5.1213123, lon: 23.123123}"))));
    }

    @Test
    public void testGetJSONPathValue(){
        String value = Helper.getJSONPathValue(ob1.toString(), "value.metrics.level");
        Assert.assertEquals("5.4", value);
    }

    @Test
    public void testGetJSONPathValueDouble(){
        Double value = new Double(Helper.getJSONPathValue(ob1.toString(), "value.metrics.level"));
        Assert.assertEquals(new Double(5.4) , value);
    }

    @Test
    public void testGetJSONPathValueString(){
        String value = Helper.getJSONPathValue(ob3.toString(), "value.metrics.level");
        Assert.assertEquals("{lat: 5.1213123, lon: 23.123123}", value);
    }

    @Test
    public void testGetJSONPathValueInteger(){
        Integer value = new Integer(Helper.getJSONPathValue(ob2.toString(), "value.metrics.level"));
        Assert.assertEquals(new Integer(5) , value);
    }

    @Test
    public void testSetJSONPathValue(){
        String path = "test.test";
        String result = Helper.setJSONPathValue(ob1.toString(), path, "4");
        Assert.assertEquals("4", JsonPath.parse(result).read("$." + path));
    }

    @Test
    public void testSetJSONPathValueDouble(){
        String path = "test.test";
        String result = Helper.setJSONPathValue(ob1.toString(), path, new Double(2.2));
        Double test = JsonPath.parse(result).read("$." + path);
        Assert.assertEquals(new Double(2.2) , test);
    }

    @Test
    public void testSetJSONPathValueTwoElements(){
        String path = "test";
        String s = Helper.setJSONPathValue(ob1.toString(), path + ".test", 2);
        s = Helper.setJSONPathValue(s, path + ".test2", 3);
        Integer test1 = JsonPath.parse(s).read("$." + path+".test");
        Integer test2 = JsonPath.parse(s).read("$." + path+".test2");
        Assert.assertEquals(new Integer(2), test1);
        Assert.assertEquals(new Integer(3), test2);
    }
}
