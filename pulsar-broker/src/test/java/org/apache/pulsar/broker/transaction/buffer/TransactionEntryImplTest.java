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
package org.apache.pulsar.broker.transaction.buffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionEntryImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.testng.annotations.Test;

/**
 * Unit test {@link TransactionEntryImpl}.
 */
@Test(groups = "broker")
public class TransactionEntryImplTest {

    @Test
    public void testCloseShouldReleaseBuffer() {
        ByteBuf buffer = Unpooled.copiedBuffer("test-value", UTF_8);
        TransactionEntryImpl entry = new TransactionEntryImpl(
                new TxnID(1234L, 3456L),
                0L,
                EntryImpl.create(-1L, -1L, buffer),
                33L,
                44L,
                1
        );
        assertEquals(buffer.refCnt(), 2);
        entry.close();
        assertEquals(buffer.refCnt(), 0);
    }

}
