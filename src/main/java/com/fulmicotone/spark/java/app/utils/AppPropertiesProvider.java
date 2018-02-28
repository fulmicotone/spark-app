package com.fulmicotone.spark.java.app.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;

public class AppPropertiesProvider implements Supplier<Properties>{

    Logger log= LoggerFactory.getLogger(AppPropertiesProvider.class);
    @Override
    public Properties get() {

        Properties appProps= new Properties();

        try {

            appProps.load(Thread.currentThread().getContextClassLoader()
                    .getResource("app.properties").openStream());
        } catch (IOException e) {
            log.error("properties file  not found",e.toString() );
        }

return appProps;
    }
}
