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

import java.util.List;

public final class InputTopicModel {

    private String name = null;
    private String filterType = null;
    private String filterValue = null;
    private String filterValue2 = null;
    private List<MappingModel> mappings = null;

    public InputTopicModel() {
    }

    public InputTopicModel(InputTopicModel another){
        this.name = another.name;
        this.filterType = another.filterType;
        this.filterValue = another.filterValue;
        this.filterValue2 = another.filterValue2;
        this.mappings = another.mappings;
    }

    public InputTopicModel(String name, String filterType, String filterValue, List<MappingModel> mappings) {
        this.name = name;
        this.filterType = filterType;
        this.filterValue = filterValue;
        this.mappings = mappings;
    }

    public String getName(){
        return this.name;
    }

    public String getFilterType(){
        return this.filterType;
    }

    public String getFilterValue(){
        return this.filterValue;
    }

    public String getFilterValue2(){
        return this.filterValue2;
    }

    public List<MappingModel> getMappings(){
        return this.mappings;
    }

    public void setMappings(List<MappingModel> mappings){
        this.mappings = mappings;
    }

    public String getSourceByDest(String dest){
        for (MappingModel mapping : this. mappings){
            if (mapping.getDest().equals(dest)){
                return mapping.getSource();
            }
        }
        return null;
    }
}
