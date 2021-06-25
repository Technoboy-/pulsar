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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.PulsarStandaloneStarter;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.socks5.Socks5Server;
import org.apache.pulsar.socks5.config.Socks5Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

@Test
public class ClientWithSocks5ProxyTest {
    private static final Logger log = LoggerFactory.getLogger(ClientWithSocks5ProxyTest.class);

    private Socks5Server server;

    private PulsarStandaloneStarter standalone;

    @BeforeMethod
    public void setup() throws Exception {
        startSocks5Server();
        startStandalone();
    }

    private void startStandalone() throws Exception {
        String args[] = new String[]{"-c",
                "./src/test/resources/configurations/pulsar_broker_socks5_standalone.conf",
                "-nfw", "-nss"};
        standalone = new PulsarStandaloneStarter(args);
        standalone.start();
    }

    private void startSocks5Server() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Socks5Config config = new Socks5Config();
        config.setPort(11080);
        config.setEnableAuth(true);
        server = new Socks5Server(config);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        latch.await(3, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        standalone.close();
        server.shutdown();
    }

    @Test
    public void testSocks5() throws PulsarClientException, PulsarAdminException {
        final String topicName = "persistent://public/default/socks5";

        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .socks5ProxyAddress(new InetSocketAddress("localhost", 11080))
                .socks5ProxyUsername("socks5")
                .socks5ProxyPassword("pulsar")
                .build();

        //
        final String subscriptionName = "socks5-subscription";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        String msg = "abc";
        producer.send(msg.getBytes());

        Message<byte[]> message = consumer.receive();

        assertEquals(new String(message.getData()), msg);

        consumer.unsubscribe();
    }

    @Test(timeOut = 10000)
    public void testSocks5WithErrorPassword() throws PulsarClientException {
        final String topicName = "persistent://public/default/socks5";
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .socks5ProxyAddress(new InetSocketAddress("localhost", 11080))
                .socks5ProxyUsername("socks5")
                .socks5ProxyPassword("pulsar123")
                .build();
        pulsarClient.newProducer()
                .topic(topicName)
                .create();

    }
}
