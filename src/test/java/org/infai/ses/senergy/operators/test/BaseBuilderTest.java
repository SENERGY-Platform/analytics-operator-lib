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

import junit.framework.TestCase;
import org.infai.ses.senergy.operators.StreamBuilder;
import org.infai.ses.senergy.utils.TimeProvider;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class BaseBuilderTest extends TestCase {

    private final LocalDateTime time = LocalDateTime.of(2020,01,01,01,01);

    @Override
    protected void setUp() throws Exception {
        TimeProvider.useFixedClockAt(time);
    }
    @Test
    public void testFormatMessage(){
        StreamBuilder builder = new StreamBuilder("1", "1");
        String message = builder.formatMessage("{'device_id': '1'}");
        Assert.assertEquals("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\"}]," +
                "\"pipeline_id\":\"1\",\"time\":\""+ TimeProvider.nowUTCToString() +"\"}",message);
    }

    @Test
    public void testFormatMessage2(){
        StreamBuilder builder = new StreamBuilder("1", "2");
        String message = builder.formatMessage("{'analytics':{'test': 1},'inputs':[{'device_id': '1'}],'pipeline_id':'1'}");
        Assert.assertEquals("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"analytics\":{\"test\":1}," +
                "\"inputs\":[{\"device_id\":\"1\"}],\"pipeline_id\":\"1\"}],\"pipeline_id\":\"2\",\"time\":\""+ TimeProvider.nowUTCToString() +"\"}",message);
    }
}
