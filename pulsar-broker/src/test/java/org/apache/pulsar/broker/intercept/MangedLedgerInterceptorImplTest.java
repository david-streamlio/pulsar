/*
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
package org.apache.pulsar.broker.intercept;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

@Test(groups = "broker")
public class MangedLedgerInterceptorImplTest  extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(MangedLedgerInterceptorImplTest.class);

    public class TestPayloadProcessor implements ManagedLedgerPayloadProcessor {
        @Override
        public Processor inputProcessor() {
            return new Processor() {
                @Override
                public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                    byte[] newMessage = (new String("Modified Test Message")).getBytes();
                    ByteBuf processedPayload =  Unpooled.wrappedBuffer(newMessage, 0, newMessage.length);
                    inputPayload.release();
                    return processedPayload.retainedDuplicate();
                }

                @Override
                public void release(ByteBuf processedPayload) {
                    processedPayload.release();
                }
            };
        }
        @Override
        public Processor outputProcessor() {
            return new Processor() {
                @Override
                public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                    byte[] bytes = new byte[inputPayload.readableBytes()];
                    inputPayload.readBytes(bytes);
                    String storedMessage = new String(bytes);
                    Assert.assertTrue(storedMessage.equals("Modified Test Message"));

                    byte[] newMessage = (new String("Test Message")).getBytes();
                    inputPayload.release();
                    return Unpooled.wrappedBuffer(newMessage, 0, newMessage.length).retainedDuplicate();
                }

                @Override
                public void release(ByteBuf processedPayload) {
                    processedPayload.release();
                }
            };
        }
    }

    @Test
    public void testAddBrokerEntryMetadata() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        int numberOfEntries = 10;
        final String ledgerAndCursorName = "topicEntryMetadataSequenceId";

        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(("message" + i).getBytes(), MOCK_BATCH_SIZE);
        }

        assertEquals(19, ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex());
        List<Entry> entryList = cursor.readEntries(numberOfEntries);
        for (int i = 0 ; i < numberOfEntries; i ++) {
            BrokerEntryMetadata metadata =
                    Commands.parseBrokerEntryMetadataIfExist(entryList.get(i).getDataBuffer());
            assertNotNull(metadata);
            assertEquals(metadata.getIndex(), (i + 1) * MOCK_BATCH_SIZE - 1);
        }

        cursor.close();
        ledger.close();
        factory.shutdown();
    }
    @Test
    public void testMessagePayloadProcessor() throws Exception {
        final String ledgerAndCursorName = "topicEntryWithPayloadProcessed";

        Set<ManagedLedgerPayloadProcessor> processors = new HashSet();
        processors.add(new TestPayloadProcessor());
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(new HashSet(),processors);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        ledger.addEntry("Test Message".getBytes());
        factory.getEntryCacheManager().clear();
        List<Entry> entryList = cursor.readEntries(1);
        String message = new String(entryList.get(0).getData());
        Assert.assertTrue(message.equals("Test Message"));
        cursor.close();
        ledger.close();
        factory.shutdown();
        config.setManagedLedgerInterceptor(null);
    }

    @Test(timeOut = 20000)
    public void testRecoveryIndex() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setManagedLedgerInterceptor(interceptor);
        ManagedLedger ledger = factory.open("my_recovery_index_test_ledger", config);

        ledger.addEntry("dummy-entry-1".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE);

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE);

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), MOCK_BATCH_SIZE * 2 - 1);

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_recovery_index_test_ledger", config);

        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), MOCK_BATCH_SIZE * 2 - 1);


        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        cursor.close();
        ledger.close();
    }

    @Test
    public void testFindPositionByIndex() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        final int maxEntriesPerLedger = 5;
        int maxSequenceIdPerLedger = MOCK_BATCH_SIZE * maxEntriesPerLedger;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);


        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setManagedLedgerInterceptor(interceptor);
        managedLedgerConfig.setMaxEntriesPerLedger(5);

        ManagedLedger ledger = factory.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);
        ManagedCursor cursor = ledger.openCursor("c1");

        long firstLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            firstLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 9);


        PositionImpl position = null;
        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }

        // roll over ledger
        long secondLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            secondLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 19);
        assertNotEquals(firstLedgerId, secondLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }

        // reopen ledger
        ledger.close();
        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);

        long thirdLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            thirdLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 29);
        assertNotEquals(secondLedgerId, thirdLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }
        cursor.close();
        ledger.close();
    }

    @Test
    public void testBeforeAddEntryWithException() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        final String ledgerAndCursorName = "testBeforeAddEntryWithException";

        ManagedLedgerInterceptor interceptor =
                new MockManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(), null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ByteBuf buffer = Unpooled.wrappedBuffer("message".getBytes());
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ledger.asyncAddEntry(buffer, MOCK_BATCH_SIZE, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    countDownLatch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    countDownLatch.countDown();
                }
            }, null);
            countDownLatch.await();
            assertEquals(buffer.refCnt(), 1);
        } finally {
            ledger.close();
            factory.shutdown();
        }
    }

    private class MockManagedLedgerInterceptorImpl extends ManagedLedgerInterceptorImpl {
        private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

        public MockManagedLedgerInterceptorImpl(
                Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors,
                Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors) {
            super(brokerEntryMetadataInterceptors, brokerEntryPayloadProcessors);
            this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;
        }

        @Override
        public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
            if (op == null || numberOfMessages <= 0) {
                return op;
            }
            op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors,
                    numberOfMessages));
            if (op != null) {
                throw new RuntimeException("throw exception before add entry for test");
            }
            return op;
        }
    }

    public static Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        return BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader());
    }

    static class IndexSearchPredicate implements Predicate<Entry> {

        long indexToSearch = -1;
        public IndexSearchPredicate(long indexToSearch) {
            this.indexToSearch = indexToSearch;
        }

        @Override
        public boolean test(@Nullable Entry entry) {
            try {
                BrokerEntryMetadata brokerEntryMetadata = Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
                return brokerEntryMetadata.getIndex() < indexToSearch;
            } catch (Exception e) {
                log.error("Error deserialize message for message position find", e);
            } finally {
                entry.release();
            }
            return false;
        }
    }

}
