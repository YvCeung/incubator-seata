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
package org.apache.seata.config.processor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;


class ProcessorPropertiesTest {

    @Test
    void processor() throws IOException {
        String properties = "registry.type=file\n" +
                "registry.file.name=file-test-pro.conf";
        
        Properties processor = new ProcessorProperties().processor(properties);
        Assertions.assertEquals("file", processor.get("registry.type"));
        // not exist
        Assertions.assertNull(processor.get("registry"));
        Assertions.assertNull(processor.get("null"));
    }

    @Test
    void processor2() throws IOException {
        String properties = "store.redis.single.port=6379\n" +
                "transport.type=TCP";

        Properties processor = new ProcessorProperties().processor(properties);
        // 整数自动会转换为字符串，但是不会丢失
        Assertions.assertEquals("6379", processor.get("store.redis.single.port"));
        Assertions.assertEquals("TCP", processor.get("transport.type"));

    }
}
