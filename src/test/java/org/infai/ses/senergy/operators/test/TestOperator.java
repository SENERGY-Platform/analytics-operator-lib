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

import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.operators.OperatorInterface;

public class TestOperator implements OperatorInterface {

    Config config;

    public TestOperator(){
        config = new Config();
    }

    public TestOperator(String configString){
        config = new Config(configString);
    }

    @Override
    public void run(Message message) {

        if (config.getConfigValue("input", "false").equals("true")){
            message.getInput("value").getValue();
        }
        if (config.getConfigValue("test", "testVal") == "test"){
            message.output("test", "2");
        } else {
            message.output("test", "1");
        }

    }

    @Override
    public void config(Message message) {
        message.addInput("value");
    }
}
