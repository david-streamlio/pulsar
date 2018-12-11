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

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AzureDataLake sink that treats incoming messages on the input topic as byte arrays
 * and write identical key/value pairs.
 */
public class AzureDataLakeSink<K, V> implements Sink<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(AzureDataLakeSink.class);

    protected ADLStoreClient client;
    private AzureDataLakeConfig azureConfig;
    private ADLFileOutputStream stream;
    private AtomicInteger streamOffset = new AtomicInteger(0);

    // Need currentFile byte counter, current file start time, and a file number counter
    private AtomicInteger currentFileSize = new AtomicInteger(0);
    private AtomicLong currentFileCreateTime;
    private AtomicInteger fileCounter = new AtomicInteger(0);

    @Override
    public void close() throws Exception {
        getStream().flush();
        getStream().close();
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        azureConfig = AzureDataLakeConfig.load(config);
        azureConfig.validate();
        AccessTokenProvider provider = new ClientCredsTokenProvider(azureConfig.getAuthTokenEndpoint(),
                azureConfig.getClientId(), azureConfig.getClientKey());

        client = ADLStoreClient.createClient(azureConfig.getAccountFQDN(), provider);

        if (client == null || !client.checkAccess(azureConfig.getPath(), "-w-")) {
            throw new IllegalArgumentException("Unable to create a client to Azure Data "
                    + "Lake with write access to " + azureConfig.getPath());
        }
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        byte[] value = extractRecordValue(record);
        getStream().write(value, streamOffset.getAndAdd(value.length), value.length);
        record.ack();
    }

    public byte[] extractRecordValue(Record<byte[]> record) {
        StringBuffer sb = new StringBuffer();
        sb.append(record.getValue()).append("\n");
        return sb.toString().getBytes();
    }

    public synchronized ADLFileOutputStream getStream() {
        String filename = getFileName();

        if (shouldRoll()) {
            rollFile();
        }

        if (stream == null) {
            try {
                if (!client.checkExists(filename)) {
                    stream = client.createFile(filename, IfExists.FAIL);
                    client.setPermission(filename, "777");
                    currentFileCreateTime.set(System.currentTimeMillis());
                } else {
                    stream = client.getAppendStream(filename);
                }

            } catch (IOException e) {
                LOG.error("Unable to access stream", e);
            }
        }
        return stream;
    }

    private String getFileName() {
        StringBuffer sb = new StringBuffer();
        sb.append(azureConfig.getPath());

        // Need to check for a path separator on the end of the path, else, we will need to add it.
        if (!azureConfig.getPath().endsWith("/")) {
            sb.append("/");
        }

        sb.append(azureConfig.getFilePrefix())
          .append("-")
          .append(fileCounter.get())
          .append(azureConfig.getFileSuffix());

        return sb.toString();
    }

    private boolean shouldRoll() {
        // If we defined a time-based policy, and we have started the timer, and the specified interval has elapsed.
        if (azureConfig.getRollInterval() > 0 && currentFileCreateTime != null
            && (System.currentTimeMillis() - currentFileCreateTime.get()) >= (azureConfig.getRollInterval() / 1000)) {
            return true;
        }

        // If we have defined a size-based policy, and the specified size has been written.
        if (azureConfig.getRollSize() > 0 && currentFileSize.get() > azureConfig.getRollSize()) {
            return true;
        }

        return false;
    }

    private void rollFile() {
        try {
            getStream().flush();
            getStream().close();
            stream = null;
            currentFileSize = new AtomicInteger(0);
            fileCounter.incrementAndGet();
        } catch (final Exception ex) {
            LOG.error("Unable to roll the file", ex);
        }
    }
}
