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
package org.apache.pulsar.broker.testcontext;

import io.netty.channel.EventLoopGroup;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * A {@link BookKeeperClientFactory} that always returns the same instance of {@link BookKeeper}.
 */
class MockBookKeeperClientFactory implements BookKeeperClientFactory {
    private final BookKeeper mockBookKeeper;

    MockBookKeeperClientFactory(BookKeeper mockBookKeeper) {
        this.mockBookKeeper = mockBookKeeper;
    }

    @Override
    public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                                    EventLoopGroup eventLoopGroup,
                                    Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                    Map<String, Object> properties) {
        // Always return the same instance (so that we don't loose the mock BK content on broker restart
        return CompletableFuture.completedFuture(mockBookKeeper);
    }

    @Override
    public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                             EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties, StatsLogger statsLogger) {
        // Always return the same instance (so that we don't loose the mock BK content on broker restart
        return CompletableFuture.completedFuture(mockBookKeeper);
    }

    @Override
    public void close() {
        // no-op
    }
}
