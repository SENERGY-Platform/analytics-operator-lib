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

import org.infai.ses.senergy.exceptions.NoValueException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FlexInput {

    private static final Logger log = Logger.getLogger(FlexInput.class.getName());

    private String currentFilterId;
    private String currentSource;
    private String currentInputTopic;
    private Boolean currentMessageProcessed = false;
    private List<Input> inputs = new LinkedList<>();

    public List<Double> getValues() {
        return getValues(Double.class);
    }

    public <T> List<T> getValues(Class<T> tClass) {
        List<T> list = new LinkedList<>();
        for (Input input : this.inputs) {
            try {
                list.add(input.getValue(tClass));
            } catch (NoValueException e) {
                log.log(Level.SEVERE, e.getMessage());
            }
        }
        return list;
    }

    public Double getValue() throws NoValueException {
        return getCurrentValue(Double.class);
    }

    public String getString() throws NoValueException {
        return getCurrentValue(String.class);
    }

    public Integer getValueAsInt() throws NoValueException {
        return getCurrentValue(Integer.class);
    }

    public <T> T getValue(Class<T> tClass) throws NoValueException {
        return getCurrentValue(tClass);
    }

    private <T> T getCurrentValue(Class<T> tClass) throws NoValueException {
        T result = null;
        for (Input input : this.inputs) {
            if (input.getCurrent()) {
                result = input.getValue(tClass);
                this.currentFilterId = input.getFilterId();
            }
        }
        return result;
    }

    public <T> Map<String, T> getFilterIdValueMap(Class<T> tClass) {
        Map<String, T> map = new HashMap<>();
        for (Input input : this.inputs) {
            try {
                map.put(input.getFilterId(), input.getValue(tClass));
            } catch (NoValueException e) {
                // ignore input
            }
        }
        return map;
    }

    public String getCurrentFilterId() {
        return getCurrentField(Input::getFilterId);
    }

    public String getCurrentSource() {
        return getCurrentField(Input::getSource);
    }

    public String getCurrentInputTopic() {
        return getCurrentField(Input::getInputTopicName);
    }

    private String getCurrentField(java.util.function.Function<Input, String> extractor) {
        String result = null;
        for (Input input : this.inputs) {
            if (input.getCurrent()) {
                result = extractor.apply(input);
            }
        }
        return result;
    }

    public void setCurrentMessageProcessed(Boolean currentMessageProcessed) {
        this.currentMessageProcessed = currentMessageProcessed;
    }

    public Boolean getCurrentMessageProcessed() {
        return currentMessageProcessed;
    }

    protected void setInputs(List<Input> inputs) {
        this.inputs = inputs;
    }

    protected List<Input> getInputs() {
        return this.inputs;
    }
}
