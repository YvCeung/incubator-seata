package org.apache.seata.common.http;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: Yu.Z
 * @create: 2025-09-24 21:14
 **/
public class HttpResponseWrapper {
    private int statusCode;
    private Map<String, List<String>> headers;
    private byte[] body; // 不用 String 保留二进制能力

    // 可选: 保留原始响应对象（方便特殊场景用）
    private Object rawResponse;

    public String bodyAsString() {
        return body != null ? new String(body, StandardCharsets.UTF_8) : null;
    }
}
