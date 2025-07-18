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

import static org.mockito.Mockito.spy;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class UnauthedAdminProxyHandlerTest extends MockedPulsarServiceBaseTest {
    private static final String STATUS_FILE_PATH = "./src/test/resources/vip_status.html";
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;
    private WebServer webServer;
    private BrokerDiscoveryProvider discoveryProvider;
    private AdminProxyWrapper adminProxyHandler;
    private PulsarResources resource;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        // enable tls and auth&auth at broker

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("localhost");
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);
        conf.setClusterName(configClusterName);

        super.init();

        // start proxy service
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerWebServiceURL(brokerUrl.toString());
        proxyConfig.setStatusFilePath(STATUS_FILE_PATH);
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        webServer = new WebServer(proxyConfig, new AuthenticationService(
                                          PulsarConfigurationLoader.convertFrom(proxyConfig)));

        resource = new PulsarResources(registerCloseable(new ZKMetadataStore(mockZooKeeper)),
                registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal)));
        discoveryProvider = spy(registerCloseable(new BrokerDiscoveryProvider(proxyConfig, resource)));
        adminProxyHandler = new AdminProxyWrapper(proxyConfig, discoveryProvider, proxyClientAuthentication);
        ServletHolder servletHolder = new ServletHolder(adminProxyHandler);
        webServer.addServlet("/admin", servletHolder);
        webServer.addServlet("/lookup", servletHolder);

        webServer.addRestResource("/", VipStatus.ATTRIBUTE_STATUS_FILE_PATH, proxyConfig.getStatusFilePath(),
                VipStatus.class);

        // start web-service
        webServer.start();

    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        webServer.stop();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    @Test
    public void testUnauthenticatedProxy() throws Exception {
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://127.0.0.1:" + webServer.getListenPortHTTP().get())
            .build();
        List<String> activeBrokers = admin.brokers().getActiveBrokers(configClusterName);
        Assert.assertEquals(activeBrokers.size(), 1);
        Assert.assertEquals(adminProxyHandler.rewrittenUrl, String.format("%s/admin/v2/brokers/%s",
                brokerUrl.toString(), configClusterName));
    }

    @Test
    public void testVipStatus() throws Exception {
        @Cleanup
        Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
        WebTarget webTarget = client.target("http://127.0.0.1:" + webServer.getListenPortHTTP().get())
                .path("/status.html");
        String response = webTarget.request().get(String.class);
        Assert.assertEquals(response, "OK");
    }

    static class AdminProxyWrapper extends AdminProxyHandler {
        String rewrittenUrl;

        AdminProxyWrapper(ProxyConfiguration config, BrokerDiscoveryProvider discoveryProvider, Authentication
                proxyClientAuthentication) {
            super(config, discoveryProvider, proxyClientAuthentication);
        }

        @Override
        protected String rewriteTarget(HttpServletRequest clientRequest) {
            rewrittenUrl = super.rewriteTarget(clientRequest);
            return rewrittenUrl;
        }

    }

}
