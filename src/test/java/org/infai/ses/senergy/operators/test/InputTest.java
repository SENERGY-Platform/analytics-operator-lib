/*
 * Copyright 2021 InfAI (CC SES)
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
import org.infai.ses.senergy.operators.Input;
import org.junit.Assert;
import org.junit.Test;

public class InputTest {

    @Test
    public void testGetValue() throws NoValueException {
        Input input = new Input();
        input.setValue("1.0");
        Assert.assertEquals( "1.0", input.getString());
        Assert.assertEquals( (Double)1.0, input.getValue());
        Assert.assertEquals( (Integer) 1, input.getValueAsInt());
        input.setValue(1);
        Assert.assertEquals( "1", input.getString());
        Assert.assertEquals( (Double)1.0, input.getValue());
        Assert.assertEquals( (Integer) 1, input.getValueAsInt());
    }

    @Test(expected=NoValueException.class)
    public void testGetValueErrorDouble() throws NoValueException {
        Input input = new Input();
        input.setValue("test");
        input.getValue();
    }

    @Test(expected=NoValueException.class)
    public void testGetValueErrorInteger() throws NoValueException {
        Input input = new Input();
        input.setValue("test");
        input.getValueAsInt();
    }
}
