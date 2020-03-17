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

public class BaseOperator implements OperatorInterface {

    protected Config config;

    public BaseOperator(){
        this.config = new Config();
    }

    @Override
    public void run(Message message) {

    }

    @Override
    public void configMessage(Message message) {

    }

    public void setConfig (String configString){
        this.config = new Config(configString);
    }

    protected Config getConfig (){
        return this.config;
    }
}
