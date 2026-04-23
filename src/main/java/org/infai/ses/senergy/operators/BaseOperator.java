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

import org.infai.ses.senergy.utils.ConfigProvider;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Properties;

public abstract class BaseOperator implements OperatorInterface {

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        try {
            Properties props = new Properties();
            props.load(Stream.class.getResourceAsStream("/app.properties"));
            System.setProperty("project.name", props.getProperty("project.name"));
        } catch (Exception e) {
            System.setProperty("project.name", "unknown");
        }
    }

    protected Config config;

    protected BaseOperator(){
        this.config = ConfigProvider.getConfig();
    }

    @Override
    public void run(Message message) {

    }

    @Override
    public Message configMessage(Message message) {
        return message;
    }

    protected Config getConfig (){
        return this.config;
    }
}
