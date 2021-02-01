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
    private boolean current = Boolean.FALSE;

    /**
     * Set the value of the input.
     *
     * @param value Object
     */
    public void setValue(Object value){
        this.value = value;
    }

    /**
     * Return the value of the input as Double.
     *
     * @return Double
     */
    public Double getValue() throws NoValueException {
        return getValue(Double.class);
    }

    /**
     * Return the value of the input as String.
     *
     * @return String
     */
    public String getString() throws NoValueException {
        return getValue(String.class);
    }

    /**
     * Return the value of the input as Integer.
     *
     * @return Integer
     */
    public Integer getValueAsInt() throws NoValueException {
        return getValue(Integer.class);
    }

    /**
     * Return the value of the input as casted type.
     *
     * @return T
     */
    private  <T>T getValue(Class<T> tClass) throws NoValueException {
        if (this.value != null){
            if (tClass.equals(String.class)){
                if (this.value instanceof String){
                    return (T) this.value;
                } else {
                    return (T) String.valueOf(this.value);
                }
            } else if (tClass.equals(Double.class)){
                if (this.value instanceof Double){
                    return (T) this.value;
                } else if (this.value instanceof Integer) {
                    Integer val = (Integer) this.value;
                    return (T) Double.valueOf(val);
                } else if (this.value instanceof String) {
                    String val =  (String) this.value;
                    try{
                        return (T) Double.valueOf(val);
                    } catch(NumberFormatException e){
                        throw new NoValueException("Cannot convert type to return value - " + e.getMessage());
                    }
                }else {
                    throw new NoValueException("Cannot convert type to return value: " + this.value.getClass().getName() + " to " + tClass.getName());
                }
            } else if (tClass.equals(Integer.class)){
                if (this.value instanceof Integer){
                    return (T) this.value;
                } else if (this.value instanceof Double) {
                    Integer val = ((Double) this.value).intValue();
                    return (T) val;
                } else if (this.value instanceof String) {
                    String string =  (String) this.value;
                    try{
                        Integer val = Double.valueOf(string).intValue();
                        return (T) val;
                    } catch(NumberFormatException e){
                        throw new NoValueException("Cannot convert type to return value - " + e.getMessage());
                    }
                }else {
                    throw new NoValueException("Cannot convert type to return value: " + this.value.getClass().getName() + " to " + tClass.getName());
                }
            }  else {
                throw new NoValueException("Cannot use type to return value: " + tClass.getName());
            }

        } else {
            throw new NoValueException("No input value is set for: " + this.source);
        }
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
     * Set the current status of the value.
     *
     * @param current Boolean
     */
    protected void setCurrent(boolean current){
        this.current = current;
    }

    /**
     * Get the current status of the input value.
     *
     * @return Boolean
     */
    protected boolean getCurrent(){
        return this.current;
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