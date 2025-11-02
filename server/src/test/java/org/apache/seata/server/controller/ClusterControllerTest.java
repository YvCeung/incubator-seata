/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.server.controller;

import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.apache.seata.common.executor.HttpCallback;
import org.apache.seata.common.holder.ObjectHolder;
import org.apache.seata.common.util.HttpClientUtil;
import org.apache.seata.server.BaseSpringBootTest;
import org.apache.seata.server.cluster.listener.ClusterChangeEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.seata.common.ConfigurationKeys.SERVER_SERVICE_PORT_CAMEL;
import static org.apache.seata.common.Constants.OBJECT_KEY_SPRING_APPLICATION_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterControllerTest extends BaseSpringBootTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());


    private static Environment environment;
    private static int port;

    @BeforeAll
    public static void setUp(ApplicationContext context) {
        environment = context.getEnvironment();
        port = Integer.parseInt(environment.getProperty(SERVER_SERVICE_PORT_CAMEL, "18091"));
    }

    @Test
    @Order(1)
    void watchTimeoutTest() throws Exception {
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        header.put(HTTP.CONN_KEEP_ALIVE, "close");
        Map<String, String> param = new HashMap<>();
        param.put("default-test", "1");
        try (CloseableHttpResponse response = HttpClientUtil.doPost(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000", param, header, 5000)) {
            if (response != null) {
                StatusLine statusLine = response.getStatusLine();
                Assertions.assertEquals(HttpStatus.SC_NOT_MODIFIED, statusLine.getStatusCode());
                return;
            }
        }
        Assertions.fail();
    }

    @Test
    @Order(2)
    void watchTimeoutTest_withHttp2() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());

        Map<String, String> params = new HashMap<>();
        params.put("default-test", "1");

        HttpCallback<Response> callback = new HttpCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                Assertions.assertNotNull(response);
                Assertions.assertEquals(Protocol.H2_PRIOR_KNOWLEDGE, response.protocol());
                Assertions.assertEquals(HttpStatus.SC_NOT_MODIFIED, response.code());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                Assertions.fail("Should not fail");
            }

            @Override
            public void onCancelled() {
                Assertions.fail("Should not be cancelled");
            }
        };

        HttpClientUtil.doPostWithHttp2(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000", params, headers, callback);
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(3)
    void watch() throws Exception {
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        Map<String, String> param = new HashMap<>();
        param.put("default-test", "1");
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ((ApplicationEventPublisher) ObjectHolder.INSTANCE.getObject(OBJECT_KEY_SPRING_APPLICATION_CONTEXT))
                        .publishEvent(new ClusterChangeEvent(this, "default-test", 2, true));
            }
        });
        thread.start();
        try (CloseableHttpResponse response =
                HttpClientUtil.doPost("http://127.0.0.1:" + port + "/metadata/v1/watch", param, header, 30000)) {
            if (response != null) {
                StatusLine statusLine = response.getStatusLine();
                Assertions.assertEquals(HttpStatus.SC_OK, statusLine.getStatusCode());
                return;
            }
        }
        Assertions.fail();
    }

    @Test
    @Order(4)
    void watch_withHttp2() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        Map<String, String> param = new HashMap<>();
        param.put("default-test", "1");

        // Simulate a cluster change event after 2 seconds
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ((ApplicationEventPublisher) ObjectHolder.INSTANCE.getObject(OBJECT_KEY_SPRING_APPLICATION_CONTEXT))
                        .publishEvent(new ClusterChangeEvent(this, "default-test", 2, true));
            }
        });
        thread.start();

        HttpCallback<Response> callback = new HttpCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                assertNotNull(response);
                Assertions.assertEquals(Protocol.H2_PRIOR_KNOWLEDGE, response.protocol());
                Assertions.assertEquals(HttpStatus.SC_OK, response.code());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                fail("Should not fail");
            }

            @Override
            public void onCancelled() {
                fail("Should not be cancelled");
            }
        };

        HttpClientUtil.doPostWithHttp2("http://127.0.0.1:" + port + "/metadata/v1/watch", param, header, callback);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(5)
    void watch_withHttp2_StreamPush() throws Exception {
        // Use thread-safe counters to track responses
        java.util.concurrent.atomic.AtomicInteger okCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger notModifiedCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger unexpectedStatusCodeCount = new java.util.concurrent.atomic.AtomicInteger(0);
        
        // Expect 2 OK responses (from 2 cluster change events) + 1 NOT_MODIFIED (timeout)
        CountDownLatch latch = new CountDownLatch(3);
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        Map<String, String> param = new HashMap<>();
        param.put("default-test", "1");

        ApplicationEventPublisher eventPublisher = 
            (ApplicationEventPublisher) ObjectHolder.INSTANCE.getObject(OBJECT_KEY_SPRING_APPLICATION_CONTEXT);

        // Simulate first cluster change event after 2 seconds
        Thread firstEventThread = new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            eventPublisher.publishEvent(new ClusterChangeEvent(this, "default-test", 2, true));
        });
        firstEventThread.start();

        // Simulate second cluster change event after 4 seconds
        Thread secondEventThread = new Thread(() -> {
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            eventPublisher.publishEvent(new ClusterChangeEvent(this, "default-test", 3, true));
        });
        secondEventThread.start();

        HttpCallback<Response> callback = new HttpCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                try {
                    assertNotNull(response, "Response should not be null");
                    Assertions.assertEquals(
                        Protocol.H2_PRIOR_KNOWLEDGE, 
                        response.protocol(), 
                        "Should use HTTP/2 protocol");
                    
                    int statusCode = response.code();
                    if (HttpStatus.SC_OK == statusCode) {
                        okCount.incrementAndGet();
                        System.out.println("收到了" + okCount.get() + "次推送");
                        latch.countDown();
                    } else if (HttpStatus.SC_NOT_MODIFIED == statusCode) {
                        notModifiedCount.incrementAndGet();
                        latch.countDown();
                    } else {
                        unexpectedStatusCodeCount.incrementAndGet();
                        fail("Unexpected status code: " + statusCode);
                        latch.countDown();
                    }
                } catch (Exception e) {
                    fail("Error processing response: " + e.getMessage(), e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                fail("HTTP/2 request should not fail: " + t.getMessage(), t);
            }

            @Override
            public void onCancelled() {
                fail("HTTP/2 request should not be cancelled");
            }
        };

        // Send watch request with 15 seconds timeout
        // Expect to receive: 2 OK responses (from cluster changes) + 1 NOT_MODIFIED (timeout)
        HttpClientUtil.doPostWithHttp2(
            "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=15000", 
            param, 
            header, 
            callback, 30);
        
        // Wait for all responses (with reasonable timeout)
        boolean completed = latch.await(20, TimeUnit.SECONDS);
        assertTrue(completed, "Should receive all expected responses within timeout");
        
        // Verify we received exactly 2 OK responses from cluster change events
        Assertions.assertEquals(
            2, 
            okCount.get(), 
            "Should receive 2 OK responses from 2 cluster change events");
        
        // Verify we received exactly 1 NOT_MODIFIED response from timeout
        Assertions.assertEquals(
            1, 
            notModifiedCount.get(), 
            "Should receive 1 NOT_MODIFIED response from timeout");
        
        // Verify no unexpected status codes
        Assertions.assertEquals(
            0, 
            unexpectedStatusCodeCount.get(), 
            "Should not receive any unexpected status codes");
    }

    @Test
    @Order(5)
    void watch_withHttp2_SingleEvent_StreamPush() throws Exception {
        // Use thread-safe counters to track responses
        java.util.concurrent.atomic.AtomicInteger okCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger notModifiedCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger unexpectedStatusCodeCount = new java.util.concurrent.atomic.AtomicInteger(0);

        // Expect 1 OK responses (from 1 cluster change events) + 1 NOT_MODIFIED (timeout)
        CountDownLatch latch = new CountDownLatch(2);
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        Map<String, String> param = new HashMap<>();
        param.put("default-test", "1");

        ApplicationEventPublisher eventPublisher =
                (ApplicationEventPublisher) ObjectHolder.INSTANCE.getObject(OBJECT_KEY_SPRING_APPLICATION_CONTEXT);

        // Simulate first cluster change event after 2 seconds
        Thread firstEventThread = new Thread(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            eventPublisher.publishEvent(new ClusterChangeEvent(this, "default-test", 2, true));
        });
        firstEventThread.start();

        HttpCallback<Response> callback = new HttpCallback<Response>() {
            @Override
            public void onSuccess(Response response) throws IOException {

                try (ResponseBody responseBody = response.body()) {
                    InputStream inputStream = responseBody.byteStream();
                    byte[] buffer = new byte[8192];
                    int bytesRead;

                    // 这里每次读取都对应着底层数据帧的到达！
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        // 每次有数据可读时执行你的回调逻辑
                        onDataChunkReceived(buffer, bytesRead);
                    }
                }
                try {
                    assertNotNull(response, "Response should not be null");
                    Assertions.assertEquals(
                            Protocol.H2_PRIOR_KNOWLEDGE,
                            response.protocol(),
                            "Should use HTTP/2 protocol");

                    int statusCode = response.code();
                    if (HttpStatus.SC_OK == statusCode) {
                        okCount.incrementAndGet();
                        logger.info("收到ok一次");
                        latch.countDown();
                    } else if (HttpStatus.SC_NOT_MODIFIED == statusCode) {
                        logger.info("收到304一次");
                        notModifiedCount.incrementAndGet();
                        latch.countDown();
                    } else {
                        unexpectedStatusCodeCount.incrementAndGet();
                        fail("Unexpected status code: " + statusCode);
                        latch.countDown();
                    }
                } catch (Exception e) {
                    fail("Error processing response: " + e.getMessage(), e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                fail("HTTP/2 request should not fail: " + t.getMessage(), t);
            }

            @Override
            public void onCancelled() {
                fail("HTTP/2 request should not be cancelled");
            }
        };

        // Send watch request with 15 seconds timeout
        // Expect to receive: 2 OK responses (from cluster changes) + 1 NOT_MODIFIED (timeout)
        HttpClientUtil.doPostWithHttp2(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=5000",
                param,
                header,
                callback, 30);

        // Wait for all responses (with reasonable timeout)
        boolean completed = latch.await(20, TimeUnit.SECONDS);
        assertTrue(completed, "Should receive all expected responses within timeout");

        // Verify we received exactly 2 OK responses from cluster change events
        Assertions.assertEquals(
                1,
                okCount.get(),
                "Should receive 2 OK responses from 2 cluster change events");

        // Verify we received exactly 1 NOT_MODIFIED response from timeout
        Assertions.assertEquals(
                1,
                notModifiedCount.get(),
                "Should receive 1 NOT_MODIFIED response from timeout");

        // Verify no unexpected status codes
        Assertions.assertEquals(
                0,
                unexpectedStatusCodeCount.get(),
                "Should not receive any unexpected status codes");
    }

    private void onDataChunkReceived(byte[] chunk, int length) {
        // 这里就是你要的"收到数据帧就执行回调逻辑"
        System.out.println("Received " + length + " bytes");
        // 处理分块数据...
    }

    @Test
    @Order(6)
    void testXssFilterBlocked_queryParam() throws Exception {
        String malicious = "<script>alert('xss')</script>";
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        try (CloseableHttpResponse response = HttpClientUtil.doGet(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000&testParam="
                        + URLEncoder.encode(malicious, String.valueOf(StandardCharsets.UTF_8)),
                new HashMap<>(),
                header,
                5000)) {
            Assertions.assertEquals(
                    HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    @Order(7)
    void testXssFilterBlocked_queryParam_withGetHttp2() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        String malicious = "<script>alert('xss')</script>";
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());

        HttpCallback<Response> callback = new HttpCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                assertNotNull(response);
                Assertions.assertEquals(Protocol.H2_PRIOR_KNOWLEDGE, response.protocol());
                Assertions.assertEquals(HttpStatus.SC_BAD_REQUEST, response.code());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                fail("Should not fail");
            }

            @Override
            public void onCancelled() {
                fail("Should not be cancelled");
            }
        };

        HttpClientUtil.doGetWithHttp2(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000&testParam="
                        + URLEncoder.encode(malicious, String.valueOf(StandardCharsets.UTF_8)),
                header,
                callback,
                5000);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(8)
    void testXssFilterBlocked_formParam_withPostHttp2() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        String malicious = "<script>alert('xss')</script>";
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());

        Map<String, String> params = new HashMap<>();
        params.put("key", malicious);

        HttpCallback<Response> callback = new HttpCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                assertNotNull(response);
                Assertions.assertEquals(Protocol.H2_PRIOR_KNOWLEDGE, response.protocol());
                Assertions.assertEquals(HttpStatus.SC_BAD_REQUEST, response.code());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                fail("Should not fail");
            }

            @Override
            public void onCancelled() {
                fail("Should not be cancelled");
            }
        };

        HttpClientUtil.doPostWithHttp2("http://127.0.0.1:" + port + "/random", params, header, callback, 5000);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(9)
    void testXssFilterBlocked_bodyParam_withPostHttp2() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        String malicious = "<script>alert('xss')</script>";
        Map<String, String> header = new HashMap<>();

        String jsonBody = "{\"key\":\"" + malicious + "\"}";

        HttpCallback<Response> callback = new HttpCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                assertNotNull(response);
                Assertions.assertEquals(Protocol.H2_PRIOR_KNOWLEDGE, response.protocol());
                Assertions.assertEquals(HttpStatus.SC_BAD_REQUEST, response.code());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                fail("Should not fail");
            }

            @Override
            public void onCancelled() {
                fail("Should not be cancelled");
            }
        };

        HttpClientUtil.doPostWithHttp2("http://127.0.0.1:" + port + "/random", jsonBody, header, callback, 5000);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(10)
    void testXssFilterBlocked_formParam() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());

        Map<String, String> params = new HashMap<>();
        params.put("testParam", "<script>alert('xss')</script>");

        try (CloseableHttpResponse response = HttpClientUtil.doPost(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000", params, headers, 5000)) {
            Assertions.assertEquals(
                    HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    @Order(11)
    void testXssFilterBlocked_jsonBody() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

        String jsonBody = "{\"testParam\":\"<script>alert('xss')</script>\"}";

        try (CloseableHttpResponse response = HttpClientUtil.doPostJson(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000", jsonBody, headers, 5000)) {
            Assertions.assertEquals(
                    HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    @Order(12)
    void testXssFilterBlocked_headerParam() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        headers.put("X-Test-Header", "<script>alert('xss')</script>");

        Map<String, String> params = new HashMap<>();
        params.put("safeParam", "123");

        try (CloseableHttpResponse response = HttpClientUtil.doPost(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000", params, headers, 5000)) {
            Assertions.assertEquals(
                    HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    @Order(13)
    void testXssFilterBlocked_multiSource() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        headers.put("X-Test-Header", "<script>alert('xss')</script>");

        String jsonBody = "{\"testParam\":\"<script>alert('xss')</script>\"}";

        try (CloseableHttpResponse response = HttpClientUtil.doPostJson(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000&urlParam="
                        + URLEncoder.encode("<script>alert('xss')</script>", String.valueOf(StandardCharsets.UTF_8)),
                jsonBody,
                headers,
                5000)) {
            Assertions.assertEquals(
                    HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    @Order(14)
    void testXssFilterBlocked_formParamWithUserCustomKeyWords() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());

        Map<String, String> params = new HashMap<>();
        params.put("testParam", "custom1");

        try (CloseableHttpResponse response = HttpClientUtil.doPost(
                "http://127.0.0.1:" + port + "/metadata/v1/watch?timeout=3000", params, headers, 5000)) {
            Assertions.assertEquals(
                    HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
        }
    }
}
