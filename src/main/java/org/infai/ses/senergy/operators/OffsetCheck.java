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

package org.infai.ses.senergy.operators;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.infai.ses.senergy.utils.ApplicationState;

import java.util.logging.Level;
import java.util.logging.Logger;

public class OffsetCheck implements Processor {

    private ProcessorContext context;
    private static final Logger log = Logger.getLogger(OffsetCheck.class.getName());

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Object o, Object o2) {
        String name = this.context.topic() + "-" + this.context.partition();
        Long currentOffset = ApplicationState.getOffset(name);
        if (currentOffset != null) {
            if (currentOffset <= this.context.offset()) {
                ApplicationState.setOffset(name, this.context.offset());
            } else {
                log.log(Level.SEVERE, "Offset is smaller than current offset: {0} < {1}.", new Object[]{this.context.offset(), currentOffset});
                System.exit(1);
            }
        } else  {
            ApplicationState.setOffset(name, this.context.offset());
        }
    }

    @Override
    public void close() {

    }
}
