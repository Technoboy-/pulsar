package org.apache.pulsar.broker.admin;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;

public class AvailableCipherSuites {

    public static void main(String[] args) throws Exception {

        // If an argument is present, then remove the
        // jdk.tls.disabledAlgorithms restrictions and
        // print all implemented cipher suites.
        if (args.length != 0) {
            Security.setProperty("jdk.tls.disabledAlgorithms", "");
        }

        SSLContext sslc = SSLContext.getDefault();
        SSLSocketFactory sslf = sslc.getSocketFactory();
        SSLSocket ssls = (SSLSocket) sslf.createSocket();

        ArrayList<String> enabled = new ArrayList(
                Arrays.asList(ssls.getEnabledCipherSuites()));

        ArrayList<String> supported = new ArrayList(
                Arrays.asList(ssls.getSupportedCipherSuites()));
        supported.removeAll(enabled);
        System.out.println("Enabled by Default Cipher Suites");
        System.out.println("--------------------------------");
        enabled.stream().forEach(System.out::println);

        System.out.println();

        System.out.println("Not Enabled by Default Cipher Suites");
        System.out.println("------------------------------------");
        supported.stream().forEach(System.out::println);
    }
}
