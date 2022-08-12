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

    private Object value;
    private String currentFilterId;
    private String currentSource;
    private String currentInputTopic;

    private Boolean currentMessageProcessed = false;

    private List<Input> inputs = new LinkedList<>();

    public List<Double> getValues() {
        List<Double> list = new LinkedList<>();
        for (Input input: this.inputs){
            try {
                list.add(input.getValue());
            } catch (NoValueException e) {
                log.log(Level.SEVERE, e.getMessage());
            }
        }
        return list;
    }

    /**
     * Return the current value of the flexInput as Double.
     *
     * @return Double
     */
    public Double getValue() throws NoValueException {
        for (Input input: this.inputs){
            if (input.getCurrent()){
                this.value= input.getValue();
                this.currentFilterId = input.getFilterId();
            }
        }
        return (Double) this.value;
    }

    /**
     * Return the current value of the flexInput as String.
     *
     * @return String
     */
    public String getString() throws NoValueException {
        for (Input input: this.inputs){
            if (input.getCurrent()){
                this.value= input.getString();
                this.currentFilterId = input.getFilterId();
            }
        }
        return (String) this.value;
    }

    /**
     * Return the current value of the flexInput as casted type.
     *
     * @return T
     */
    public  <T>T getValue(Class<T> tClass) throws NoValueException {
        for (Input input: this.inputs){
            if (input.getCurrent()){
                this.value = input.getValue(tClass);
                this.currentFilterId = input.getFilterId();
            }
        }
        return (T) this.value;
    }

    /**
     * Return the current filterId and value of the each input as casted type.
     * Inputs without values or values that can not be casted are excluded.
     *
     * @return Map<String, T>
     */
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

    /**
     * Return the current value of the flexInput as Integer.
     *
     * @return Integer
     */
    public Integer getValueAsInt() throws NoValueException {
        for (Input input: this.inputs){
            if (input.getCurrent()){
                this.value= input.getValueAsInt();
                this.currentFilterId = input.getFilterId();
            }
        }
        return (Integer) this.value;
    }

    /**
     * Returns the filter id of the current value.
     *
     * @return String
     */
    public String getCurrentFilterId(){
        this.currentFilterId = null;
        for (Input input: this.inputs){
            if (input.getCurrent()){
                this.currentFilterId = input.getFilterId();
            }
        }
        return this.currentFilterId;
    }

    public String getCurrentSource(){
        for (Input input: this.inputs){
            if (input.getCurrent()){
                this.currentSource = input.getSource();
            }
        }
        return this.currentSource;
    }

    public String getCurrentInputTopic(){
        for (Input input: this.inputs){
            if (input.getCurrent()){
                this.currentInputTopic = input.getInputTopicName();
            }
        }
        return this.currentInputTopic;
    }

    public void setCurrentMessageProcessed(Boolean currentMessageProcessed) {
        this.currentMessageProcessed = currentMessageProcessed;
    }

    public Boolean getCurrentMessageProcessed() {
        return currentMessageProcessed;
    }

    protected void setInputs(List<Input> inputs){
        this.inputs = inputs;
    }

    protected List<Input> getInputs(){
        return this.inputs;
    }
}
