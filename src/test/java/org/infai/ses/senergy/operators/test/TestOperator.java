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

import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

public class TestOperator extends BaseOperator {


    @Override
    public void run(Message message) {
        switch (getConfig().getConfigValue("value", "")) {
            case "2inputs":
                try {
                    message.output("val", message.getInput("value").getValue().toString());
                    message.output("val2", message.getInput("value2").getValue().toString());
                } catch (NoValueException e) {

                }
                break;
            default:
                message.output("test", "1");
                break;
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addInput("value");
        message.addInput("value2");
        return message;
    }
}
