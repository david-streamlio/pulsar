/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.azure.datalake;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import org.apache.commons.lang3.StringUtils;

/**
 * Configuration class for the Azure Data Lake Sink Connector.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class AzureDataLakeConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String clientId;
    private String authTokenEndpoint;
    private String clientKey;
    private String accountFQDN;
    private String path;

    /**
     * Name prefixed to files created.
     */
    private String filePrefix;

    /**
     * Suffix to append to file.
     */
    private String fileSuffix;

    /**
     * Number of seconds to wait before rolling current file.
     */
    private long rollInterval = 0;

    /**
     * File size to trigger roll, in bytes (0: never roll based on file size).
     */
    private long rollSize = 0;

    public static AzureDataLakeConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), AzureDataLakeConfig.class);
    }

    public static AzureDataLakeConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), AzureDataLakeConfig.class);
    }

    public void validate() {
        if (StringUtils.isBlank(accountFQDN) || StringUtils.isBlank(authTokenEndpoint)
            || StringUtils.isBlank(clientId) || StringUtils.isBlank(clientKey)
            || StringUtils.isBlank(path) || StringUtils.isBlank(filePrefix)) {
            throw new IllegalArgumentException("Required Property Not Set.");
        }
    }
}
