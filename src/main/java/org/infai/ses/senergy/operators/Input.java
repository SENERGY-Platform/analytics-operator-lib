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

public class Input {

    private Object value;
    private String source = "";
    private String inputTopic = "";
    private String filterId = "";

    /**
     * Set the value of the input.
     *
     * @param value Object
     */
    protected void setValue(Object value){
        this.value = value;
    }

    /**
     * Return the value of the input as Double.
     *
     * @return Double
     */
    public Double getValue() throws NoValueException {
        if (this.value != null){
            if (this.value instanceof String){
                return Double.valueOf((String) this.value);
            } else if (this.value instanceof Integer){
                return Double.valueOf((Integer) this.value);
            } else if (this.value instanceof Double){
                return (Double) this.value;
            }
        } else {
            throw new NoValueException("No input value is set for: " + this.source);
        }
        return null;
    }

    /**
     * Return the value of the input as String.
     *
     * @return String
     */
    public String getString(){
        try {
            if (this.value instanceof Integer) {
                return this.value.toString();
            } else
            return (String) this.value;
        } catch (NullPointerException | ClassCastException e){
            return "";
        }
    }

    /**
     * Set the filterId of the input.
     *
     * @param filterId String
     */
    protected void setFilterId(String filterId){
        this.filterId = filterId;
    }

    /**
     * Get the current filterId of the input.
     *
     * @return String
     */
    public String getFilterId() {
        return filterId;
    }

    /**
     * Set the source mapping of the input.
     *
     * @param source String
     */
    protected void setSource(String source){
        this.source = source;
    }

    /**
     * Get the source of the input.
     *
     * @return String
     */
    protected String getSource(){
        return this.source;
    }

    /**
     * Set the inputTopic name of the input.
     *
     * @param inputTopic String
     */
    protected void setInputTopicName(String inputTopic){
        this.inputTopic = inputTopic;
    }

    /**
     * Get the input topic name of the input.
     *
     * @return String
     */
    protected String getInputTopicName(){
        return this.inputTopic;
    }
}