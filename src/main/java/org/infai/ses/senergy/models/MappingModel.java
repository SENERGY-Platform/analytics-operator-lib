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

package org.infai.ses.senergy.models;

public final class MappingModel {

    private String source = null;
    private String dest = null;

    public MappingModel(){
    }

    public MappingModel(String source, String dest){
        this.source = source;
        this.dest = dest;
    }

    public String getSource () {
        return this.source;
    }

    public String getDest(){
        return this.dest;
    }
}
