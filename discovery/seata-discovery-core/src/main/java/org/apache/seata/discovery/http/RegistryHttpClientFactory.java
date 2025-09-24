package org.apache.seata.discovery.http;

/**
 * @description:
 * @author: Yu.Z
 * @create: 2025-09-24 12:19
 **/
public class RegistryHttpClientFactory {

    private static RegistryHttpClient INSTANCE;

    static {
        try {
            // 尝试加载 okhttp3 类
            Class.forName("okhttp3.OkHttpClient");
            INSTANCE = new Http2RegistryHttpClient();
            System.out.println("Detected OkHttp3, using HTTP/2 client.");
        } catch (ClassNotFoundException e) {
            INSTANCE = new Http1RegistryHttpClient();
            System.out.println("OkHttp3 not found, falling back to HTTP/1 client.");
        }
    }

    // 此方法使用前 需要充分考虑 server端是否支持http2 协议， 目前是否支持的判断是放在上层调用方做的
    public static RegistryHttpClient getClient() {
        return INSTANCE;
    }
}
