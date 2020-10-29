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

import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

public class TestFlexOperator extends BaseOperator {

    @Override
    public void run(Message message) {
        if (config.getConfigValue("test", "1").equals("1")){
            Double result = 0.0;
            for (Double value : message.getFlexInput("value").getValues()){
                result += value;
            };
            message.output("test", result);
        } else if (config.getConfigValue("test", "1").equals("2")){
            Double result = 0.0;
            Double result2 = 0.0;
            for (Double value : message.getFlexInput("value").getValues()){
                result += value;
            };
            for (Double value : message.getFlexInput("value2").getValues()){
                result2 -= value;
            };
            message.output("test", result);
            message.output("test2", result2);
        }

    }

    @Override
    public Message configMessage(Message message) {
        if (config.getConfigValue("test", "1").equals("1")){
            message.addFlexInput("value");
        }else if (config.getConfigValue("test", "1").equals("2")) {
            message.addFlexInput("value");
            message.addFlexInput("value2");
        }
        return message;
    }
}
