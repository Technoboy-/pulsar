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
package org.apache.pulsar.proxy.server;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.doReturn;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class ProxyKeyStoreTlsWithoutAuthTest extends MockedPulsarServiceBaseTest {
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(false);

        proxyConfig.setTlsEnabledWithKeyStore(true);
        proxyConfig.setTlsKeyStoreType(KEYSTORE_TYPE);
        proxyConfig.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        proxyConfig.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
        proxyConfig.setTlsTrustStoreType(KEYSTORE_TYPE);
        proxyConfig.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
        proxyConfig.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);
        proxyConfig.setTlsRequireTrustedClientCertOnConnect(true);

        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper)))
                .when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();

        proxyService.start();
    }

    protected PulsarClient internalSetUpForClient(boolean addCertificates, String lookupUrl) throws Exception {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(lookupUrl)
                .enableTls(true)
                .useKeyStoreTls(true)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .allowTlsInsecureConnection(false)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        if (addCertificates) {
            Map<String, String> authParams = new HashMap<>();
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_TYPE, KEYSTORE_TYPE);
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, CLIENT_KEYSTORE_FILE_PATH);
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PW, CLIENT_KEYSTORE_PW);
            clientBuilder.authentication(AuthenticationKeyStoreTls.class.getName(), authParams);
        }
        return clientBuilder.build();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        proxyService.close();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    @Test
    public void testProducer() throws Exception {
        @Cleanup
        PulsarClient client = internalSetUpForClient(true, proxyService.getServiceUrlTls());
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://sample/test/local/topic" + System.currentTimeMillis())
                .create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
    }

    @Test
    public void testProducerFailed() throws Exception {
        @Cleanup
        PulsarClient client = internalSetUpForClient(false, proxyService.getServiceUrlTls());
        try {
            @Cleanup
            Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic("persistent://sample/test/local/topic" + System.currentTimeMillis())
                    .create();
            Assert.fail("Should failed since broker setTlsRequireTrustedClientCertOnConnect, "
                        + "while client not set keystore");
        } catch (Exception e) {
            // expected
            log.info("Expected failed since broker setTlsRequireTrustedClientCertOnConnect,"
                     + " while client not set keystore");
        }
    }

    @Test
    public void testPartitions() throws Exception {
        @Cleanup
        PulsarClient client = internalSetUpForClient(true, proxyService.getServiceUrlTls());
        String topicName = "persistent://sample/test/local/partitioned-topic" + System.currentTimeMillis();
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("sample", tenantInfo);
        admin.topics().createPartitionedTopic(topicName, 2);

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(topicName)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // Create a consumer directly attached to broker
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            requireNonNull(msg);
        }
    }

}
