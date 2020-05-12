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

import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.infai.ses.senergy.operators.Helper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class HelperIntegrationTest {

    @Test
    public void testGetBrokerList() throws IOException, InterruptedException {
        EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        Assert.assertEquals(cluster.bootstrapServers(), Helper.getBrokerList(cluster.zKConnectString()));
        cluster.deleteAllTopicsAndWait(2000);
    }
}
