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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class SubscriptionBacklogAgePerformanceTest extends MockedPulsarServiceBaseTest {

    private static final String NAMESPACE = "public/perf-sub-backlog-age";
    private static final int WARMUP_RUNS = 3;
    private static final int MEASURE_RUNS = 8;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setExposeSubscriptionBacklogAgeInPrometheus(false);
        conf.setPreciseTimeBasedBacklogQuotaCheck(false);
        conf.setBacklogQuotaCheckIntervalInSeconds(3600);
        conf.setManagedLedgerMaxEntriesPerLedger(1);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        super.internalSetup();
        admin.clusters().createCluster(configClusterName, ClusterData.builder().build());
        admin.tenants().createTenant("public",
                TenantInfo.builder().allowedClusters(Set.of(configClusterName)).build());
        admin.namespaces().createNamespace(NAMESPACE);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "subscriptionCounts")
    public Object[][] subscriptionCounts() {
        return new Object[][] {
                {20_000},
                {50_000}
        };
    }

    @Test(dataProvider = "subscriptionCounts", timeOut = 30 * 60 * 1000L)
    public void measureUpdateOldPositionInfoWithManySubscriptions(int subscriptionCount) throws Exception {
        String topicName = "persistent://" + NAMESPACE + "/topic-" + subscriptionCount;
        PersistentTopic topic = createTopicWithBackloggedSubscriptions(topicName, subscriptionCount);

        BenchmarkResult disabled = measureRepeated("disabled", topic, false);
        BenchmarkResult firstPass = measureSingle("enabled-first-pass", topic, true);
        BenchmarkResult cacheHit = measureRepeated("enabled-cache-hit", topic, true);
        TopicStats stats = admin.topics().getStats(topicName);
        long populatedBacklogAgeCount = stats.getSubscriptions().values().stream()
                .filter(s -> s.getOldestBacklogMessageAgeSeconds() >= 0)
                .count();

        System.out.printf("%nSUBSCRIPTION_BACKLOG_AGE_PERF subscriptionCount=%d%n", subscriptionCount);
        System.out.printf("usedHeapMb=%d%n", usedHeapMb());
        System.out.println(disabled);
        System.out.println(firstPass);
        System.out.println(cacheHit);
        System.out.printf("statsSubscriptionCount=%d populatedBacklogAgeCount=%d%n",
                stats.getSubscriptions().size(), populatedBacklogAgeCount);
        System.out.printf("sampleOldestBacklogAgeSeconds=%d%n",
                stats.getSubscriptions().get("sub-0").getOldestBacklogMessageAgeSeconds());
    }

    private PersistentTopic createTopicWithBackloggedSubscriptions(String topicName, int subscriptionCount)
            throws Exception {
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, true)
                .get(30, TimeUnit.SECONDS).orElseThrow();

        long start = System.nanoTime();
        for (int i = 0; i < subscriptionCount; i++) {
            topic.createSubscription("sub-" + i, InitialPosition.Earliest, false, null)
                    .get(30, TimeUnit.SECONDS);
            if (i > 0 && i % 5_000 == 0) {
                log.info()
                        .attr("topic", topicName)
                        .attr("createdSubscriptions", i)
                        .attr("usedHeapMb", usedHeapMb())
                        .log("Created subscriptions for backlog age performance test");
            }
        }
        System.out.printf("createdSubscriptions=%d elapsedMs=%d%n",
                subscriptionCount, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        MessageId id1 = producer.send(new byte[] {1});
        MessageId id2 = producer.send(new byte[] {2});
        assertTrue(id1 != null);
        assertTrue(id2 != null);

        start = System.nanoTime();
        for (int i = 0; i < subscriptionCount; i++) {
            topic.getSubscription("sub-" + i).getCursor().resetCursor(PositionFactory.EARLIEST);
        }
        System.out.printf("resetSubscriptionsToEarliest=%d elapsedMs=%d%n",
                subscriptionCount, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return topic;
    }

    private BenchmarkResult measureSingle(String name, PersistentTopic topic, boolean exposeSubscriptionBacklogAge)
            throws Exception {
        conf.setExposeSubscriptionBacklogAgeInPrometheus(exposeSubscriptionBacklogAge);

        long start = System.nanoTime();
        topic.updateOldPositionInfo().get(60, TimeUnit.SECONDS);
        return BenchmarkResult.from(name, List.of(System.nanoTime() - start));
    }

    private BenchmarkResult measureRepeated(String name, PersistentTopic topic, boolean exposeSubscriptionBacklogAge)
            throws Exception {
        conf.setExposeSubscriptionBacklogAgeInPrometheus(exposeSubscriptionBacklogAge);

        for (int i = 0; i < WARMUP_RUNS; i++) {
            topic.updateOldPositionInfo().get(60, TimeUnit.SECONDS);
        }

        List<Long> elapsedNanos = new ArrayList<>(MEASURE_RUNS);
        for (int i = 0; i < MEASURE_RUNS; i++) {
            long start = System.nanoTime();
            topic.updateOldPositionInfo().get(60, TimeUnit.SECONDS);
            elapsedNanos.add(System.nanoTime() - start);
        }
        return BenchmarkResult.from(name, elapsedNanos);
    }

    private static long usedHeapMb() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }

    private record BenchmarkResult(String name, long minMs, long avgMs, long p95Ms, long maxMs) {
        static BenchmarkResult from(String name, List<Long> elapsedNanos) {
            List<Long> sorted = elapsedNanos.stream().sorted(Comparator.naturalOrder()).toList();
            long sum = elapsedNanos.stream().mapToLong(Long::longValue).sum();
            long p95 = sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * 0.95) - 1));
            return new BenchmarkResult(
                    name,
                    TimeUnit.NANOSECONDS.toMillis(sorted.get(0)),
                    TimeUnit.NANOSECONDS.toMillis(sum / elapsedNanos.size()),
                    TimeUnit.NANOSECONDS.toMillis(p95),
                    TimeUnit.NANOSECONDS.toMillis(sorted.get(sorted.size() - 1)));
        }

        @Override
        public String toString() {
            return String.format("%s minMs=%d avgMs=%d p95Ms=%d maxMs=%d", name, minMs, avgMs, p95Ms, maxMs);
        }
    }
}
