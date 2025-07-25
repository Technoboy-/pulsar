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
package org.apache.pulsar.io.kafka.source;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.KafkaAbstractSource;
import org.apache.pulsar.io.kafka.KafkaSourceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaAbstractSourceTest {

    private static class DummySource extends KafkaAbstractSource<String> {

        @Override
        public KafkaRecord<String> buildRecord(ConsumerRecord<Object, Object> consumerRecord) {
            return new KafkaRecord<>(consumerRecord,
                    new String((byte[]) consumerRecord.value(), StandardCharsets.UTF_8),
                    Schema.STRING,
                    Collections.emptyMap());
        }
    }

    @Test
    public void testInvalidConfigWillThrownException() throws Exception {
        KafkaAbstractSource<String> source = new DummySource();
        SourceContext ctx = mock(SourceContext.class);
        Map<String, Object> config = new HashMap<>();
        Assert.ThrowingRunnable openAndClose = ()->{
            try {
                source.open(config, ctx);
                fail();
            } finally {
                source.close();
            }
        };
        expectThrows(NullPointerException.class, openAndClose);
        config.put("topic", "topic_1");
        expectThrows(NullPointerException.class, openAndClose);
        config.put("bootstrapServers", "localhost:8080");
        expectThrows(NullPointerException.class, openAndClose);
        config.put("groupId", "test-group");
        config.put("fetchMinBytes", -1);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("fetchMinBytes", 1000);
        config.put("autoCommitEnabled", true);
        config.put("autoCommitIntervalMs", -1);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("autoCommitIntervalMs", 100);
        config.put("sessionTimeoutMs", -1);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("sessionTimeoutMs", 10000);
        config.put("heartbeatIntervalMs", -100);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("heartbeatIntervalMs", 20000);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("heartbeatIntervalMs", 5000);
        config.put("autoOffsetReset", "some-value");
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("autoOffsetReset", "earliest");
        source.open(config, ctx);
        source.close();
    }

    @Test
    public void loadConsumerConfigPropertiesFromMapTest() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("consumerConfigProperties", "");
        config.put("bootstrapServers", "localhost:8080");
        config.put("groupId", "test-group");
        config.put("topic", "test-topic");
        SourceContext sourceContext = Mockito.mock(SourceContext.class);
        KafkaSourceConfig kafkaSourceConfig = KafkaSourceConfig.load(config, sourceContext);
        assertNotNull(kafkaSourceConfig);
        assertNull(kafkaSourceConfig.getConsumerConfigProperties());

        config.put("consumerConfigProperties", null);
        kafkaSourceConfig = KafkaSourceConfig.load(config, sourceContext);
        assertNull(kafkaSourceConfig.getConsumerConfigProperties());

        config.put("consumerConfigProperties", ImmutableMap.of("foo", "bar"));
        kafkaSourceConfig = KafkaSourceConfig.load(config, sourceContext);
        assertEquals(kafkaSourceConfig.getConsumerConfigProperties(), ImmutableMap.of("foo", "bar"));
    }

    @Test
    public void loadSensitiveFieldsFromSecretTest() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("consumerConfigProperties", "");
        config.put("bootstrapServers", "localhost:8080");
        config.put("groupId", "test-group");
        config.put("topic", "test-topic");
        SourceContext sourceContext = Mockito.mock(SourceContext.class);
        Mockito.when(sourceContext.getSecret("sslTruststorePassword"))
                .thenReturn("xxxx");
        KafkaSourceConfig kafkaSourceConfig = KafkaSourceConfig.load(config, sourceContext);
        assertNotNull(kafkaSourceConfig);
        assertNull(kafkaSourceConfig.getConsumerConfigProperties());
        assertEquals("xxxx", kafkaSourceConfig.getSslTruststorePassword());
    }

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("kafkaSourceConfig.yaml");
        KafkaSourceConfig config = KafkaSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals("localhost:6667", config.getBootstrapServers());
        assertEquals("test", config.getTopic());
        assertEquals(Long.parseLong("10000"), config.getSessionTimeoutMs());
        assertFalse(config.isAutoCommitEnabled());
        assertEquals("latest", config.getAutoOffsetReset());
        assertNotNull(config.getConsumerConfigProperties());
        Properties props = new Properties();
        props.putAll(config.getConsumerConfigProperties());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        assertEquals("test-pulsar-consumer", props.getProperty("client.id"));
        assertEquals("SASL_PLAINTEXT", props.getProperty("security.protocol"));
        assertEquals("test-pulsar-io", props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    public final void loadFromSaslYamlFileTest() throws IOException {
        File yamlFile = getFile("kafkaSourceConfigSasl.yaml");
        KafkaSourceConfig config = KafkaSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals(config.getBootstrapServers(), "localhost:6667");
        assertEquals(config.getTopic(), "test");
        assertEquals(config.getSecurityProtocol(), SecurityProtocol.SASL_PLAINTEXT.name);
        assertEquals(config.getSaslMechanism(), "PLAIN");
        assertEquals(config.getSaslJaasConfig(), "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required \nusername=\"alice\" \npassword=\"pwd\";");
        assertEquals(config.getSslEndpointIdentificationAlgorithm(), "");
        assertEquals(config.getSslTruststoreLocation(), "/etc/cert.pem");
        assertEquals(config.getSslTruststorePassword(), "cert_pwd");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Subscribe exception")
    public final void throwExceptionBySubscribe() throws Exception {
        KafkaAbstractSource<String> source = new DummySource();

        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        kafkaSourceConfig.setTopic("test-topic");
        Field kafkaSourceConfigField = KafkaAbstractSource.class.getDeclaredField("kafkaSourceConfig");
        kafkaSourceConfigField.setAccessible(true);
        kafkaSourceConfigField.set(source, kafkaSourceConfig);

        Consumer<Object, Object> consumer = mock(Consumer.class);
        Mockito.doThrow(new RuntimeException("Subscribe exception")).when(consumer)
                .subscribe(Mockito.anyCollection());

        Field consumerField = KafkaAbstractSource.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(source, consumer);
        // will throw RuntimeException.
        source.start();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Pool exception")
    public final void throwExceptionByPoll() throws Exception {
        KafkaAbstractSource<String> source = new DummySource();

        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        kafkaSourceConfig.setTopic("test-topic");
        Field kafkaSourceConfigField = KafkaAbstractSource.class.getDeclaredField("kafkaSourceConfig");
        kafkaSourceConfigField.setAccessible(true);
        kafkaSourceConfigField.set(source, kafkaSourceConfig);

        Consumer<Object, Object> consumer = mock(Consumer.class);
        Mockito.doThrow(new RuntimeException("Pool exception")).when(consumer)
                .poll(Mockito.any(Duration.class));

        Field consumerField = KafkaAbstractSource.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(source, consumer);
        source.start();
        // will throw RuntimeException.
        source.read();
    }

    @Test
    public final void throwExceptionBySendFail() throws Exception {
        KafkaAbstractSource source = new DummySource();

        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        kafkaSourceConfig.setTopic("test-topic");
        kafkaSourceConfig.setAutoCommitEnabled(false);
        Field kafkaSourceConfigField = KafkaAbstractSource.class.getDeclaredField("kafkaSourceConfig");
        kafkaSourceConfigField.setAccessible(true);
        kafkaSourceConfigField.set(source, kafkaSourceConfig);

        Field defaultMaxPollIntervalMsField = KafkaAbstractSource.class.getDeclaredField("maxPollIntervalMs");
        defaultMaxPollIntervalMsField.setAccessible(true);
        defaultMaxPollIntervalMsField.set(source, 300000);

        Consumer consumer = mock(Consumer.class);
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>("topic", 0, 0,
                "t-key", "t-value".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<String, byte[]> consumerRecords = new ConsumerRecords<>(Collections.singletonMap(
                new TopicPartition("topic", 0),
                Arrays.asList(consumerRecord)));
        Mockito.doReturn(consumerRecords).when(consumer).poll(Mockito.any(Duration.class));

        Field consumerField = KafkaAbstractSource.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(source, consumer);
        source.start();

        // Mock send message fail
        Record record = source.read();
        record.fail();

        // read again will throw RuntimeException.
        try {
            source.read();
            fail("Should throw exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().contains("Failed to process record with kafka topic"));
        }
    }

    @Test
    public final void throwExceptionBySendTimeOut() throws Exception {
        KafkaAbstractSource source = new DummySource();

        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        kafkaSourceConfig.setTopic("test-topic");
        kafkaSourceConfig.setAutoCommitEnabled(false);
        Field kafkaSourceConfigField = KafkaAbstractSource.class.getDeclaredField("kafkaSourceConfig");
        kafkaSourceConfigField.setAccessible(true);
        kafkaSourceConfigField.set(source, kafkaSourceConfig);

        Field defaultMaxPollIntervalMsField = KafkaAbstractSource.class.getDeclaredField("maxPollIntervalMs");
        defaultMaxPollIntervalMsField.setAccessible(true);
        defaultMaxPollIntervalMsField.set(source, 1);

        Consumer consumer = mock(Consumer.class);
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>("topic", 0, 0,
                "t-key", "t-value".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<String, byte[]> consumerRecords = new ConsumerRecords<>(Collections.singletonMap(
                new TopicPartition("topic", 0),
                Arrays.asList(consumerRecord)));
        Mockito.doReturn(consumerRecords).when(consumer).poll(Mockito.any(Duration.class));

        Field consumerField = KafkaAbstractSource.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(source, consumer);
        source.start();

        // Mock send message fail, just read do noting.
        source.read();

        // read again will throw TimeOutException.
        try {
            source.read();
            fail("Should throw exception");
        } catch (Exception e) {
            assertTrue(e instanceof TimeoutException);
        }
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
