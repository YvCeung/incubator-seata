package org.apache.seata.common.http;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @Description:
 * @Author: Yu.Z
 * @Create: 2025-09-24
 **/
public interface HttpClient {

    CompletableFuture<HttpResponseWrapper> doGet(
            String url, Map<String, String> headers, int timeout
    );

    CompletableFuture<HttpResponseWrapper> doPost(
            String url, Map<String, String> params, Map<String, String> headers, int timeout
    );

    CompletableFuture<HttpResponseWrapper> doPostJson(
            String url, String jsonBody, Map<String, String> headers, int timeout
    );
}
