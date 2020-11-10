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

import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.serialization.JSONDeserializer;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.junit.Assert;
import org.junit.Test;


public class DeserializerTest {

    @Test
    public void testDeserialize(){
        JSONDeserializer deserializer = new JSONDeserializer(AnalyticsMessageModel.class);
        AnalyticsMessageModel model = (AnalyticsMessageModel) deserializer.deserialize("", JSONHelper.readFileAsBytes("deserializer/message-1.json"));
        Assert.assertEquals("b5ac827b-0197-441d-a4ea-4965e37c80b6",model.getOperatorId());
    }
}
