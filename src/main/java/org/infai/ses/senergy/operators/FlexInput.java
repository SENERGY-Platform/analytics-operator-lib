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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class FlexInput {

    private List<Input> inputs = new LinkedList<>();

    public List<Double> getValues() {
        List<Double> list = new LinkedList<>();
        for (Input input: this.inputs){
            try {
                list.add(input.getValue());
            } catch (NoValueException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    protected void setInputs(List<Input> inputs){
        this.inputs = inputs;
    }

    protected List<Input> getInputs(){
        return this.inputs;
    }
}
