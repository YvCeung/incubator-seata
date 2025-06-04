package org.apache.seata.server.inspector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;

@Component
public class PropertySourceInspector {

    @Autowired
    private Environment environment;

    private static final String TARGET_KEY = "lockMode";

    @PostConstruct
    public void inspect() {
        if (environment instanceof ConfigurableEnvironment) {
            ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) environment;

            for (PropertySource<?> propertySource : configurableEnvironment.getPropertySources()) {
                Object source = propertySource.getSource();
                if (source instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) source;
                    if (map.containsKey(TARGET_KEY)) {
                        System.out.println("[Found in Map] " + propertySource.getName() + " = " + map.get(TARGET_KEY));
                    }
                }

                Object value = propertySource.getProperty(TARGET_KEY);
                if (value != null) {
                    System.out.println("[Found via getProperty()] " + propertySource.getName() + " = " + value);
                }
            }

            // 再输出一遍从 Environment 获取的值
            System.out.println("Environment says: " + TARGET_KEY + " = " + environment.getProperty(TARGET_KEY));
        }
    }
}


