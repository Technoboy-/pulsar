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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil;
import org.apache.pulsar.broker.storage.BookkeeperManagedLedgerStorageClass;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.common.util.PortManager;
import org.apache.pulsar.compaction.CompactionServiceFactory;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.PulsarCompactionServiceFactory;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.MockZooKeeperSession;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.jspecify.annotations.NonNull;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;

/**
 * A test context that can be used to set up a Pulsar broker and associated resources.
 *
 * There are 2 types of Pulsar unit tests that use a PulsarService:
 * <ul>
 * <li>Some Pulsar unit tests use a PulsarService that isn't started</li>
 * <li>Some Pulsar unit tests start the PulsarService and use less mocking</li>
 * </ul>
 *
 * This class can be used to set up a PulsarService that can be used in both types of tests.
 *
 * There are few motivations for PulsarTestContext:
 * <ul>
 * <li>It reduces the reliance on Mockito for hooking into the PulsarService for injecting mocks or customizing the
 * behavior of some collaborators. Mockito is not thread-safe and some mocking operations get corrupted. Some examples
 * of the issues: https://github.com/apache/pulsar/issues/13620, https://github.com/apache/pulsar/issues/16444 and https://github.com/apache/pulsar/issues/16427.</li>
 * <li>Since the Mockito issue causes test flakiness, this change will improve reliability.</li>
 * <li>It makes it possible to use composition over inheritance in test classes. This can help reduce the dependency on
 * deep test base cases hierarchies.</li>
 * <li>It reduces code duplication across test classes.</li>
 * </ul>
 *
 * <h2>Example usage of a PulsarService that is started</h2>
 * <pre>{@code
 * PulsarTestContext testContext = PulsarTestContext.builder()
 *     .spyByDefault()
 *     .withMockZooKeeper()
 *     .build();
 * PulsarService pulsarService = testContext.getPulsarService();
 * try {
 *     // Do some testing
 * } finally {
 *     testContext.close();
 * }
 * }</pre>
 *
 * <h2>Example usage of a PulsarService that is not started at all</h2>
 * <pre>{@code
 * PulsarTestContext testContext = PulsarTestContext.builderForNonStartableContext()
 *     .spyByDefault()
 *     .build();
 * PulsarService pulsarService = testContext.getPulsarService();
 * try {
 *     // Do some testing
 * } finally {
 *     testContext.close();
 * }
 * }</pre>
 */
@Slf4j
@ToString
@Getter
@Builder(builderClassName = "Builder")
public class PulsarTestContext implements AutoCloseable {
    private final ServiceConfiguration config;
    private final MetadataStoreExtended localMetadataStore;
    private final MetadataStoreExtended configurationMetadataStore;
    private final PulsarResources pulsarResources;

    private final OrderedExecutor executor;

    private final ManagedLedgerStorage managedLedgerStorage;

    private final PulsarService pulsarService;

    private final Compactor compactor;

    private final CompactionServiceFactory compactionServiceFactory;

    private final BrokerService brokerService;

    @Getter(AccessLevel.NONE)
    @Singular("registerCloseable")
    private final List<AutoCloseable> closeables;

    private final BrokerInterceptor brokerInterceptor;

    private final BookKeeper bookKeeperClient;

    private final MockZooKeeper mockZooKeeper;

    private final MockZooKeeper mockZooKeeperGlobal;

    private final TestZKServer testZKServer;

    private final TestZKServer testZKServerGlobal;

    private final SpyConfig spyConfig;

    private final boolean startable;

    private final boolean preallocatePorts;

    private final boolean enableOpenTelemetry;
    private final InMemoryMetricReader openTelemetryMetricReader;

    public ManagedLedgerStorage getManagedLedgerStorage() {
        return managedLedgerStorage;
    }

    public ManagedLedgerFactory getDefaultManagedLedgerFactory() {
        return getManagedLedgerStorage().getDefaultStorageClass().getManagedLedgerFactory();
    }

    public PulsarMockBookKeeper getMockBookKeeper() {
        return PulsarMockBookKeeper.class.cast(bookKeeperClient);
    }

    /**
     * Create a builder for a PulsarTestContext that creates a PulsarService that
     * is started.
     *
     * @return a builder for a PulsarTestContext
     */
    public static Builder builder() {
        return new StartableCustomBuilder();
    }

    /**
     * Create a builder for a PulsarTestContext that creates a PulsarService that
     * cannot be started. Some tests need to create a PulsarService that cannot be started.
     * This was added when this type of tests were migrated to use PulsarTestContext.
     *
     * @return a builder for a PulsarTestContext that cannot be started.
     */
    public static Builder builderForNonStartableContext() {
        return new NonStartableCustomBuilder();
    }

    /**
     * Close the PulsarTestContext and all the resources that it created.
     *
     * @throws Exception if there is an error closing the resources
     */
    public void close() throws Exception {
        callCloseables(closeables);
    }

    private static void callCloseables(List<AutoCloseable> closeables) {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception e) {
                log.error("Failure in calling cleanup function", e);
            }
        }
    }

    /**
     * Create a ServerCnx instance that has a Mockito spy that is recording invocations.
     * This is useful for tests for ServerCnx.
     *
     * @return a ServerCnx instance
     */
    public ServerCnx createServerCnxSpy() {
        return BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations(ServerCnx.class,
                getPulsarService());
    }

    private enum WithMockZooKeeperOrTestZKServer {
        MOCKZOOKEEPER, MOCKZOOKEEPER_SEPARATE_GLOBAL, TEST_ZK_SERVER, TEST_ZK_SERVER_SEPARATE_GLOBAL;

        boolean isMockZooKeeper() {
            return this == MOCKZOOKEEPER || this == MOCKZOOKEEPER_SEPARATE_GLOBAL;
        }

        boolean isTestZKServer() {
            return this == TEST_ZK_SERVER || this == TEST_ZK_SERVER_SEPARATE_GLOBAL;
        }
    }

    /**
     * A builder for a PulsarTestContext.
     *
     * The builder is created with Lombok. That is the reason why the builder source code doesn't show all behaviors
     */
    public static class Builder {
        protected boolean useTestPulsarResources = false;
        protected MetadataStore pulsarResourcesMetadataStore;
        protected SpyConfig.Builder spyConfigBuilder = SpyConfig.builder(SpyConfig.SpyType.NONE);
        protected Consumer<PulsarService> pulsarServiceCustomizer;
        protected ServiceConfiguration svcConfig = initializeConfig();
        protected Consumer<ServiceConfiguration> configOverrideCustomizer;

        protected boolean configOverrideCalled = false;
        protected Function<BrokerService, BrokerService> brokerServiceCustomizer = Function.identity();
        protected PulsarTestContext otherContextToClose;
        protected WithMockZooKeeperOrTestZKServer withMockZooKeeperOrTestZKServer;

        /**
         * Initialize the ServiceConfiguration with default values.
         */
        protected ServiceConfiguration initializeConfig() {
            ServiceConfiguration svcConfig = new ServiceConfiguration();
            defaultOverrideServiceConfiguration(svcConfig);
            return svcConfig;
        }

        /**
         * Some settings like the broker shutdown timeout and thread pool sizes are
         * overridden if the provided values are the default values.
         * This is used to run tests with smaller thread pools and shorter timeouts by default.
         * You can use <pre>{@code .configCustomizer(null)}</pre> to disable this behavior
         */
        protected void defaultOverrideServiceConfiguration(ServiceConfiguration svcConfig) {
            ServiceConfiguration unconfiguredDefaults = new ServiceConfiguration();

            // adjust brokerShutdownTimeoutMs if it is the default value or if it is 0
            if (svcConfig.getBrokerShutdownTimeoutMs() == unconfiguredDefaults.getBrokerShutdownTimeoutMs()
                    || svcConfig.getBrokerShutdownTimeoutMs() == 0L) {
                svcConfig.setBrokerShutdownTimeoutMs(resolveBrokerShutdownTimeoutMs());
            }

            // adjust thread pool sizes if they are the default values

            if (svcConfig.getNumIOThreads() == unconfiguredDefaults.getNumIOThreads()) {
                svcConfig.setNumIOThreads(4);
            }
            if (svcConfig.getNumOrderedExecutorThreads() == unconfiguredDefaults.getNumOrderedExecutorThreads()) {
                // use a single threaded ordered executor by default
                svcConfig.setNumOrderedExecutorThreads(1);
            }
            if (svcConfig.getNumExecutorThreadPoolSize() == unconfiguredDefaults.getNumExecutorThreadPoolSize()) {
                svcConfig.setNumExecutorThreadPoolSize(5);
            }
            if (svcConfig.getNumCacheExecutorThreadPoolSize()
                    == unconfiguredDefaults.getNumCacheExecutorThreadPoolSize()) {
                svcConfig.setNumCacheExecutorThreadPoolSize(2);
            }
            if (svcConfig.getNumHttpServerThreads() == unconfiguredDefaults.getNumHttpServerThreads()) {
                svcConfig.setNumHttpServerThreads(8);
            }

            // change the default value for ports so that a random port is used
            if (unconfiguredDefaults.getBrokerServicePort().equals(svcConfig.getBrokerServicePort())) {
                svcConfig.setBrokerServicePort(Optional.of(0));
            }
            if (unconfiguredDefaults.getWebServicePort().equals(svcConfig.getWebServicePort())) {
                svcConfig.setWebServicePort(Optional.of(0));
            }

            // change the default value for nic speed
            if (unconfiguredDefaults.getLoadBalancerOverrideBrokerNicSpeedGbps()
                    .equals(svcConfig.getLoadBalancerOverrideBrokerNicSpeedGbps())) {
                svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            }

            // set the cluster name if it's unset
            if (svcConfig.getClusterName() == null) {
                svcConfig.setClusterName("test");
            }

            // adjust managed ledger cache size
            if (svcConfig.getManagedLedgerCacheSizeMB() == unconfiguredDefaults.getManagedLedgerCacheSizeMB()) {
                svcConfig.setManagedLedgerCacheSizeMB(8);
            }

            if (svcConfig.getTopicLoadTimeoutSeconds() == unconfiguredDefaults.getTopicLoadTimeoutSeconds()) {
                svcConfig.setTopicLoadTimeoutSeconds(10);
            }
        }

        /**
         * Internal method used in the {@link StartableCustomBuilder} to override the default value for the broker
         * shutdown timeout.
         * @return the broker shutdown timeout in milliseconds
         */
        protected long resolveBrokerShutdownTimeoutMs() {
            return 0L;
        }

        /**
         * Configure the PulsarService instance and
         * the PulsarService collaborator objects to use Mockito spies by default.
         * @see SpyConfig
         * @return the builder
         */
        public Builder spyByDefault() {
            spyConfigDefault(SpyConfig.SpyType.SPY);
            return this;
        }

        public Builder spyNoneByDefault() {
            spyConfigDefault(SpyConfig.SpyType.NONE);
            return this;
        }

        public Builder spyConfigDefault(SpyConfig.SpyType spyType) {
            spyConfigBuilder = SpyConfig.builder(spyType);
            return this;
        }

        public Builder spyConfigCustomizer(Consumer<SpyConfig.Builder> spyConfigCustomizer) {
            spyConfigCustomizer.accept(spyConfigBuilder);
            return this;
        }

        /**
         * Customize the ServiceConfiguration object that is used to configure the PulsarService instance.
         * @param configCustomerizer the function to customize the ServiceConfiguration instance
         * @return the builder
         */
        public Builder configCustomizer(Consumer<ServiceConfiguration> configCustomerizer) {
            configCustomerizer.accept(svcConfig);
            if (config != null) {
                configCustomerizer.accept(config);
            }
            return this;
        }

        /**
         * Override configuration values in the ServiceConfiguration instance as a last step.
         * There are default overrides provided by
         * {@link PulsarTestContext.Builder#defaultOverrideServiceConfiguration(ServiceConfiguration)}
         * that can be disabled by using <pre>{@code .configOverride(null)}</pre>
         *
         * @param configOverrideCustomizer the function that contains overrides to ServiceConfiguration,
         *                                 set to null to disable default overrides
         * @return the builder
         */
        public Builder configOverride(Consumer<ServiceConfiguration> configOverrideCustomizer) {
            this.configOverrideCustomizer = configOverrideCustomizer;
            this.configOverrideCalled = true;
            return this;
        }

        /**
         * Customize the PulsarService instance.
         * @param pulsarServiceCustomizer the function to customize the PulsarService instance
         * @return the builder
         */
        public Builder pulsarServiceCustomizer(
                Consumer<PulsarService> pulsarServiceCustomizer) {
            this.pulsarServiceCustomizer = pulsarServiceCustomizer;
            return this;
        }

        /**
         * Reuses the BookKeeper client and metadata stores from another PulsarTestContext.
         * @param otherContext the other PulsarTestContext
         * @return the builder
         */
        public Builder reuseMockBookkeeperAndMetadataStores(PulsarTestContext otherContext) {
            bookKeeperClient(otherContext.getBookKeeperClient());
            if (otherContext.getMockZooKeeper() != null) {
                withMockZooKeeperOrTestZKServer = null;
                mockZooKeeper(otherContext.getMockZooKeeper());
                if (otherContext.getMockZooKeeperGlobal() != null) {
                    mockZooKeeperGlobal(otherContext.getMockZooKeeperGlobal());
                }
            } else if (otherContext.getTestZKServer() != null) {
                withMockZooKeeperOrTestZKServer = null;
                testZKServer(otherContext.getTestZKServer());
                if (otherContext.getTestZKServerGlobal() != null) {
                    testZKServerGlobal(otherContext.getTestZKServerGlobal());
                }
            } else {
                localMetadataStore(NonClosingProxyHandler.createNonClosingProxy(otherContext.getLocalMetadataStore(),
                        MetadataStoreExtended.class
                ));
                configurationMetadataStore(NonClosingProxyHandler.createNonClosingProxy(
                        otherContext.getConfigurationMetadataStore(), MetadataStoreExtended.class
                ));
            }
            return this;
        }

        /**
         * Reuses the {@link SpyConfig} from another PulsarTestContext.
         * @param otherContext the other PulsarTestContext
         * @return the builder
         */
        public Builder reuseSpyConfig(PulsarTestContext otherContext) {
            spyConfigBuilder = otherContext.getSpyConfig().toBuilder();
            return this;
        }

        /**
         * Chains closing of the other PulsarTestContext to this one.
         * The other PulsarTestContext will be closed when this one is closed.
         */
        public Builder chainClosing(PulsarTestContext otherContext) {
            otherContextToClose = otherContext;
            return this;
        }

        /**
         * Registers a closeable to close as the last one by prepending it to the closeables list.
         */
        public Builder prependCloseable(AutoCloseable closeable) {
            closeables.add(0, closeable);
            return this;
        }

        /**
         * Configure this PulsarTestContext to use a mock ZooKeeper instance which is
         * shared for both the local and configuration metadata stores.
         *
         * @return the builder
         */
        public Builder withMockZookeeper() {
            return withMockZookeeper(false);
        }

        /**
         * Configure this PulsarTestContext to use a mock ZooKeeper instance.
         *
         * @param useSeparateGlobalZk if true, the global (configuration) zookeeper will be a separate instance
         * @return the builder
         */
        public Builder withMockZookeeper(boolean useSeparateGlobalZk) {
            if (useSeparateGlobalZk) {
                withMockZooKeeperOrTestZKServer = WithMockZooKeeperOrTestZKServer.MOCKZOOKEEPER_SEPARATE_GLOBAL;
            } else {
                withMockZooKeeperOrTestZKServer = WithMockZooKeeperOrTestZKServer.MOCKZOOKEEPER;
            }
            return this;
        }


        /**
         * Configure this PulsarTestContext to use a test ZooKeeper instance which is
         * shared for both the local and configuration metadata stores.
         *
         * @return the builder
         */
        public Builder withTestZookeeper() {
            return withTestZookeeper(false);
        }

        /**
         * Configure this PulsarTestContext to use a test ZooKeeper instance.
         *
         * @param useSeparateGlobalZk if true, the global (configuration) zookeeper will be a separate instance
         * @return the builder
         */
        public Builder withTestZookeeper(boolean useSeparateGlobalZk) {
            if (useSeparateGlobalZk) {
                withMockZooKeeperOrTestZKServer = WithMockZooKeeperOrTestZKServer.TEST_ZK_SERVER_SEPARATE_GLOBAL;
            } else {
                withMockZooKeeperOrTestZKServer = WithMockZooKeeperOrTestZKServer.TEST_ZK_SERVER;
            }
            return this;
        }

        /**
         * Applicable only when PulsarTestContext is not startable. This will configure mocks
         * for PulsarTestResources and related classes.
         *
         * @return the builder
         */
        public Builder useTestPulsarResources() {
            if (startable) {
                throw new IllegalStateException("Cannot useTestPulsarResources when startable.");
            }
            useTestPulsarResources = true;
            return this;
        }

        /**
         * Applicable only when PulsarTestContext is not startable. This will configure mocks
         * for PulsarTestResources and related collaborator instances.
         * The {@link NamespaceResources} and {@link TopicResources} instances will use the provided
         * {@link MetadataStore} instance.
         * @param metadataStore the metadata store to use
         * @return the builder
         */
        public Builder useTestPulsarResources(MetadataStore metadataStore) {
            if (startable) {
                throw new IllegalStateException("Cannot useTestPulsarResources when startable.");
            }
            useTestPulsarResources = true;
            pulsarResourcesMetadataStore = metadataStore;
            return this;
        }

        /**
         * Applicable only when PulsarTestContext is not startable. This will configure the {@link BookKeeper}
         * and {@link ManagedLedgerFactory} instances to use for creating a {@link ManagedLedgerStorage} instance
         * for PulsarService.
         *
         * @param bookKeeperClient the bookkeeper client to use (mock bookkeeper)
         * @param managedLedgerFactory the managed ledger factory to use (could be a mock)
         * @return the builder
         */
        public Builder managedLedgerClients(BookKeeper bookKeeperClient,
                                            ManagedLedgerFactory managedLedgerFactory) {
            return managedLedgerStorage(
                    PulsarTestContext.createManagedLedgerStorage(bookKeeperClient, managedLedgerFactory));
        }

        /**
         * Configures a function to use for customizing the {@link BrokerService} instance when it gets created.
         * @return the builder
         */
        public Builder brokerServiceCustomizer(Function<BrokerService, BrokerService> brokerServiceCustomizer) {
            this.brokerServiceCustomizer = brokerServiceCustomizer;
            return this;
        }
    }

    /**
     * Internal class that contains customizations for the Lombok generated Builder.
     *
     * With Lombok, it is necessary to extend the generated Builder class for adding customizations related to
     * instantiation and completing the builder.
     */
    abstract static class AbstractCustomBuilder extends Builder {
        AbstractCustomBuilder(boolean startable) {
            super.startable = startable;
        }

        public Builder startable(boolean startable) {
            throw new UnsupportedOperationException("Cannot change startability after builder creation.");
        }

        @Override
        public final PulsarTestContext build() {
            SpyConfig spyConfig = spyConfigBuilder.build();
            spyConfig(spyConfig);
            if (super.config == null) {
                config(svcConfig);
            }
            handlePreallocatePorts(super.config);
            if (configOverrideCustomizer != null || !configOverrideCalled) {
                // call defaultOverrideServiceConfiguration if configOverrideCustomizer
                // isn't explicitly set to null with `.configOverride(null)` call
                defaultOverrideServiceConfiguration(super.config);
            }
            if (configOverrideCustomizer != null) {
                configOverrideCustomizer.accept(super.config);
            }
            createWithMockZooKeeperOrTestZKServerInstances();
            if (super.managedLedgerStorage != null && !MockUtil.isMock(super.managedLedgerStorage)) {
                super.managedLedgerStorage = spyConfig.getManagedLedgerStorage().spy(super.managedLedgerStorage);
            }
            initializeCommonPulsarServices(spyConfig);
            initializePulsarServices(spyConfig, this);
            if (pulsarServiceCustomizer != null) {
                pulsarServiceCustomizer.accept(super.pulsarService);
            }
            if (super.startable) {
                try {
                    super.pulsarService.start();
                } catch (Exception e) {
                    callCloseables(super.closeables);
                    super.closeables.clear();
                    throw new RuntimeException(e);
                }
            }
            if (otherContextToClose != null) {
                prependCloseable(otherContextToClose);
            }
            brokerService(super.pulsarService.getBrokerService());
            return super.build();
        }

        void createWithMockZooKeeperOrTestZKServerInstances() {
            if (withMockZooKeeperOrTestZKServer == null) {
                return;
            }
            int sessionTimeout = (int) super.config.getMetadataStoreSessionTimeoutMillis();
            try {
                if (withMockZooKeeperOrTestZKServer.isMockZooKeeper()) {
                    if (super.mockZooKeeper == null) {
                        mockZooKeeper(createMockZooKeeper(sessionTimeout));
                    } else {
                        log.warn("Skipping creating mockZooKeeper, already set");
                    }
                    if (withMockZooKeeperOrTestZKServer
                            == WithMockZooKeeperOrTestZKServer.MOCKZOOKEEPER_SEPARATE_GLOBAL) {
                        if (super.mockZooKeeperGlobal == null) {
                            mockZooKeeperGlobal(createMockZooKeeper(sessionTimeout));
                        } else {
                            log.warn("Skipping creating mockZooKeeperGlobal, already set");
                        }
                    }
                } else if (withMockZooKeeperOrTestZKServer.isTestZKServer()) {
                    if (super.testZKServer == null) {
                        testZKServer(createTestZookeeper(sessionTimeout));
                    } else {
                        log.warn("Skipping creating testZKServer, already set");
                    }
                    if (withMockZooKeeperOrTestZKServer
                            == WithMockZooKeeperOrTestZKServer.TEST_ZK_SERVER_SEPARATE_GLOBAL) {
                        if (super.testZKServerGlobal == null) {
                            testZKServerGlobal(createTestZookeeper(sessionTimeout));
                        } else {
                            log.warn("Skipping creating testZKServerGlobal, already set");
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private MockZooKeeper createMockZooKeeper(int sessionTimeout) throws Exception {
            MockZooKeeper zk = MockZooKeeper.newInstance();
            zk.setSessionTimeout(sessionTimeout);
            initializeZookeeper(zk);
            registerCloseable(zk::shutdown);
            return zk;
        }

        // this might not be required at all, but it's kept here as an example
        private static void initializeZookeeper(ZooKeeper zk) throws KeeperException, InterruptedException {
            ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                    "".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }

        private TestZKServer createTestZookeeper(int sessionTimeout) throws Exception {
            TestZKServer testZKServer = new TestZKServer();
            try (ZooKeeper zkc = new ZooKeeper(testZKServer.getConnectionString(), sessionTimeout, event -> {
            })) {
                initializeZookeeper(zkc);
            }
            registerCloseable(testZKServer);
            return testZKServer;
        }

        protected void handlePreallocatePorts(ServiceConfiguration config) {
            if (super.preallocatePorts) {
                config.getBrokerServicePort().ifPresent(portNumber -> {
                    if (portNumber == 0) {
                        config.setBrokerServicePort(Optional.of(PortManager.nextLockedFreePort()));
                    }
                });
                config.getBrokerServicePortTls().ifPresent(portNumber -> {
                    if (portNumber == 0) {
                        config.setBrokerServicePortTls(Optional.of(PortManager.nextLockedFreePort()));
                    }
                });
                config.getWebServicePort().ifPresent(portNumber -> {
                    if (portNumber == 0) {
                        config.setWebServicePort(Optional.of(PortManager.nextLockedFreePort()));
                    }
                });
                config.getWebServicePortTls().ifPresent(portNumber -> {
                    if (portNumber == 0) {
                        config.setWebServicePortTls(Optional.of(PortManager.nextLockedFreePort()));
                    }
                });
                registerCloseable(() -> {
                    config.getBrokerServicePort().ifPresent(PortManager::releaseLockedPort);
                    config.getBrokerServicePortTls().ifPresent(PortManager::releaseLockedPort);
                    config.getWebServicePort().ifPresent(PortManager::releaseLockedPort);
                    config.getWebServicePortTls().ifPresent(PortManager::releaseLockedPort);
                });
            }
        }

        private void initializeCommonPulsarServices(SpyConfig spyConfig) {
            if (super.bookKeeperClient == null && super.managedLedgerStorage == null) {
                if (super.executor == null) {
                    OrderedExecutor createdExecutor = OrderedExecutor.newBuilder().numThreads(1)
                            .name(PulsarTestContext.class.getSimpleName() + "-executor").build();
                    registerCloseable(() -> GracefulExecutorServicesShutdown.initiate()
                            .timeout(Duration.ZERO)
                            .shutdown(createdExecutor)
                            .handle().get());
                    super.executor = createdExecutor;
                }
                NonClosableMockBookKeeper mockBookKeeper;
                try {
                    mockBookKeeper =
                            spyConfig.getBookKeeperClient().spy(NonClosableMockBookKeeper.class, super.executor);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                registerCloseable(() -> {
                    mockBookKeeper.reallyShutdown();
                    resetSpyOrMock(mockBookKeeper);
                });
                bookKeeperClient(mockBookKeeper);
            }
            if (super.bookKeeperClient == null && super.managedLedgerStorage != null) {
                bookKeeperClient(super.managedLedgerStorage.getStorageClasses().stream()
                        .filter(BookkeeperManagedLedgerStorageClass.class::isInstance)
                        .map(BookkeeperManagedLedgerStorageClass.class::cast)
                        .map(BookkeeperManagedLedgerStorageClass::getBookKeeperClient).findFirst().get());
            }
            if (super.localMetadataStore == null || super.configurationMetadataStore == null) {
                if (super.mockZooKeeper != null) {
                    MetadataStoreExtended mockZookeeperMetadataStore =
                            createMockZookeeperMetadataStore(super.mockZooKeeper, super.config,
                                    MetadataStoreConfig.METADATA_STORE);
                    if (super.localMetadataStore == null) {
                        localMetadataStore(mockZookeeperMetadataStore);
                    }
                    if (super.configurationMetadataStore == null) {
                        if (super.mockZooKeeperGlobal != null) {
                            configurationMetadataStore(createMockZookeeperMetadataStore(super.mockZooKeeperGlobal,
                                    super.config, MetadataStoreConfig.CONFIGURATION_METADATA_STORE));
                        } else {
                            configurationMetadataStore(mockZookeeperMetadataStore);
                        }
                    }
                } else if (super.testZKServer != null) {
                    MetadataStoreExtended testZookeeperMetadataStore =
                            createTestZookeeperMetadataStore(super.testZKServer, super.config,
                                    MetadataStoreConfig.METADATA_STORE);
                    if (super.localMetadataStore == null) {
                        localMetadataStore(testZookeeperMetadataStore);
                    }
                    if (super.configurationMetadataStore == null) {
                        if (super.testZKServerGlobal != null) {
                            configurationMetadataStore(createTestZookeeperMetadataStore(super.testZKServerGlobal,
                                    super.config, MetadataStoreConfig.CONFIGURATION_METADATA_STORE));
                        } else {
                            configurationMetadataStore(testZookeeperMetadataStore);
                        }
                    }
                } else {
                    try {
                        MetadataStoreExtended store = MetadataStoreFactoryImpl.createExtended("memory:local",
                                MetadataStoreConfig.builder()
                                        .metadataStoreName(MetadataStoreConfig.METADATA_STORE).build());
                        registerCloseable(() -> {
                            store.close();
                            resetSpyOrMock(store);
                        });
                        MetadataStoreExtended nonClosingProxy =
                                NonClosingProxyHandler.createNonClosingProxy(store, MetadataStoreExtended.class
                                );
                        if (super.localMetadataStore == null) {
                            localMetadataStore(nonClosingProxy);
                        }
                        if (super.configurationMetadataStore == null) {
                            configurationMetadataStore(nonClosingProxy);
                        }
                    } catch (MetadataStoreException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private MetadataStoreConfig createMetadataStoreConfig(ServiceConfiguration config, String metadataStoreName) {
            return MetadataStoreConfig.builder()
                    .sessionTimeoutMillis((int) config.getMetadataStoreSessionTimeoutMillis())
                    .allowReadOnlyOperations(config.isMetadataStoreAllowReadOnlyOperations())
                    .batchingEnabled(config.isMetadataStoreBatchingEnabled())
                    .batchingMaxDelayMillis(config.getMetadataStoreBatchingMaxDelayMillis())
                    .batchingMaxOperations(config.getMetadataStoreBatchingMaxOperations())
                    .batchingMaxSizeKb(config.getMetadataStoreBatchingMaxSizeKb())
                    .metadataStoreName(metadataStoreName)
                    .build();
        }

        private MetadataStoreExtended createMockZookeeperMetadataStore(MockZooKeeper mockZooKeeper,
                                                                       ServiceConfiguration config,
                                                                       String metadataStoreName) {
            // provide a unique session id for each instance
            MockZooKeeperSession mockZooKeeperSession = MockZooKeeperSession.newInstance(mockZooKeeper, false);
            mockZooKeeperSession.setSessionTimeout((int) config.getMetadataStoreSessionTimeoutMillis());
            registerCloseable(() -> {
                mockZooKeeperSession.close();
                resetSpyOrMock(mockZooKeeperSession);
            });
            ZKMetadataStore zkMetadataStore =
                    new ZKMetadataStore(mockZooKeeperSession, createMetadataStoreConfig(config, metadataStoreName));
            registerCloseable(() -> {
                zkMetadataStore.close();
                resetSpyOrMock(zkMetadataStore);
            });
            MetadataStoreExtended nonClosingProxy =
                    NonClosingProxyHandler.createNonClosingProxy(zkMetadataStore, MetadataStoreExtended.class);
            return nonClosingProxy;
        }

        @SneakyThrows
        private MetadataStoreExtended createTestZookeeperMetadataStore(TestZKServer zkServer,
                                                                       ServiceConfiguration config,
                                                                       String metadataStoreName) {
            MetadataStoreExtended store = MetadataStoreExtended.create("zk:" + zkServer.getConnectionString(),
                    createMetadataStoreConfig(config, metadataStoreName));
            registerCloseable(store);
            MetadataStoreExtended nonClosingProxy =
                    NonClosingProxyHandler.createNonClosingProxy(store, MetadataStoreExtended.class);
            return nonClosingProxy;
        }

        protected abstract void initializePulsarServices(SpyConfig spyConfig, Builder builder);
    }

    static void resetSpyOrMock(Object object) {
        if (MockUtil.isMock(object)) {
            Mockito.reset(object);
        }
    }

    /**
     * Customizations for a builder for creating a PulsarTestContext that is "startable".
     */
    static class StartableCustomBuilder extends AbstractCustomBuilder {
        StartableCustomBuilder() {
            super(true);
        }

        @Override
        public Builder managedLedgerStorage(ManagedLedgerStorage managedLedgerStorage) {
            throw new IllegalStateException("Cannot set managedLedgerStorage when startable.");
        }

        @Override
        public Builder pulsarResources(PulsarResources pulsarResources) {
            throw new IllegalStateException("Cannot set pulsarResources when startable.");
        }

        @Override
        protected void initializePulsarServices(SpyConfig spyConfig, Builder builder) {
            BookKeeperClientFactory bookKeeperClientFactory =
                    new MockBookKeeperClientFactory(builder.bookKeeperClient);
            CompactionServiceFactory compactionServiceFactory = builder.compactionServiceFactory;
            if (builder.compactionServiceFactory == null && builder.config.getCompactionServiceFactoryClassName()
                    .equals(PulsarCompactionServiceFactory.class.getName())) {
                compactionServiceFactory = new MockPulsarCompactionServiceFactory(spyConfig, builder.compactor);
            }
            Consumer<AutoConfiguredOpenTelemetrySdkBuilder> openTelemetrySdkBuilderCustomizer;
            if (builder.enableOpenTelemetry) {
                var reader = InMemoryMetricReader.create();
                openTelemetryMetricReader(reader);
                registerCloseable(reader);
                openTelemetrySdkBuilderCustomizer =
                        BrokerOpenTelemetryTestUtil.getOpenTelemetrySdkBuilderConsumer(reader);
            } else {
                openTelemetrySdkBuilderCustomizer = null;
            }
            PulsarService pulsarService = spyConfig.getPulsarService()
                    .spy(StartableTestPulsarService.class, spyConfig, builder.config, builder.localMetadataStore,
                            builder.configurationMetadataStore, compactionServiceFactory,
                            builder.brokerInterceptor,
                            bookKeeperClientFactory, builder.brokerServiceCustomizer,
                            openTelemetrySdkBuilderCustomizer);
            if (compactionServiceFactory != null) {
                compactionServiceFactory.initialize(pulsarService);
            }
            registerCloseable(() -> {
                pulsarService.close();
                resetSpyOrMock(pulsarService);
            });
            pulsarService(pulsarService);
        }

        @Override
        protected long resolveBrokerShutdownTimeoutMs() {
            // wait 5 seconds for the startable pulsar service to shutdown gracefully
            // this reduces the chances that some threads of the PulsarTestsContexts of subsequent test executions
            // are running in parallel. It doesn't prevent it completely.
            return 5000L;
        }
    }

    /**
     * Customizations for a builder for creating a PulsarTestContext that is "non-startable".
     */
    static class NonStartableCustomBuilder extends AbstractCustomBuilder {

        NonStartableCustomBuilder() {
            super(false);
        }

        @Override
        protected void initializePulsarServices(SpyConfig spyConfig, Builder builder) {
            if (builder.managedLedgerStorage == null) {
                ManagedLedgerFactory mlFactoryMock = Mockito.mock(ManagedLedgerFactory.class);
                managedLedgerStorage(
                        spyConfig.getManagedLedgerStorage()
                                .spy(PulsarTestContext.createManagedLedgerStorage(builder.bookKeeperClient,
                                        mlFactoryMock)));
            }
            if (builder.pulsarResources == null) {
                SpyConfig.SpyType spyConfigPulsarResources = spyConfig.getPulsarResources();
                if (useTestPulsarResources) {
                    MetadataStore metadataStore = pulsarResourcesMetadataStore;
                    if (metadataStore == null) {
                        metadataStore = builder.configurationMetadataStore;
                    }
                    NamespaceResources nsr = spyConfigPulsarResources.spy(NamespaceResources.class,
                            metadataStore, 30);
                    TopicResources tsr = spyConfigPulsarResources.spy(TopicResources.class, metadataStore);
                    pulsarResources(
                            spyConfigPulsarResources.spy(
                                    NonStartableTestPulsarService.TestPulsarResources.class, builder.localMetadataStore,
                                    builder.configurationMetadataStore,
                                    tsr, nsr));
                } else {
                    pulsarResources(
                            spyConfigPulsarResources.spy(PulsarResources.class, builder.localMetadataStore,
                                    builder.configurationMetadataStore));
                }
            }
            BookKeeperClientFactory bookKeeperClientFactory =
                    new MockBookKeeperClientFactory(builder.bookKeeperClient);
            CompactionServiceFactory compactionServiceFactory = builder.compactionServiceFactory;
            if (builder.compactionServiceFactory == null && builder.config.getCompactionServiceFactoryClassName()
                    .equals(PulsarCompactionServiceFactory.class.getName())) {
                compactionServiceFactory = new MockPulsarCompactionServiceFactory(spyConfig, builder.compactor);
            }
            PulsarService pulsarService = spyConfig.getPulsarService()
                    .spy(NonStartableTestPulsarService.class, spyConfig, builder.config, builder.localMetadataStore,
                            builder.configurationMetadataStore, compactionServiceFactory,
                            builder.brokerInterceptor,
                            bookKeeperClientFactory, builder.pulsarResources,
                            builder.managedLedgerStorage, builder.brokerServiceCustomizer);
            if (compactionServiceFactory != null) {
                compactionServiceFactory.initialize(pulsarService);
            }
            registerCloseable(() -> {
                pulsarService.close();
                resetSpyOrMock(pulsarService);
            });
            pulsarService(pulsarService);
        }
    }

    @NonNull
    private static ManagedLedgerStorage createManagedLedgerStorage(BookKeeper bookKeeperClient,
                                                                   ManagedLedgerFactory managedLedgerFactory) {
        BookkeeperManagedLedgerStorageClass managedLedgerStorageClass =
                new BookkeeperManagedLedgerStorageClass() {
                    @Override
                    public String getName() {
                        return "bookkeeper";
                    }

                    @Override
                    public ManagedLedgerFactory getManagedLedgerFactory() {
                        return managedLedgerFactory;
                    }

                    @Override
                    public StatsProvider getStatsProvider() {
                        return new NullStatsProvider();
                    }

                    @Override
                    public BookKeeper getBookKeeperClient() {
                        return bookKeeperClient;
                    }
                };
        return new ManagedLedgerStorage() {
            @Override
            public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                                   BookKeeperClientFactory bookkeeperProvider, EventLoopGroup eventLoopGroup,
                                   OpenTelemetry openTelemetry) {
            }

            @Override
            public Collection<ManagedLedgerStorageClass> getStorageClasses() {
                return List.of(managedLedgerStorageClass);
            }

            @Override
            public Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name) {
                if (name == null || name.equals("bookkeeper")) {
                    return Optional.of(managedLedgerStorageClass);
                } else {
                    return Optional.empty();
                }
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
