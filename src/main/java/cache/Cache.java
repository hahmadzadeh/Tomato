package cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import loader.LoadGraphToPlatform;

public class Cache {

    protected Properties properties;
    protected int cacheSize;
    protected int cacheSizeMsg;
    protected AtomicInteger counter;
    public Cache() {
        this.properties = new Properties();
        try (
            InputStream in = LoadGraphToPlatform.class.getResourceAsStream("/config.properties")) {
            System.out.println(in);
            properties.load(in);
            this.cacheSize =  Integer.parseInt(properties.getProperty("cacheSize"));
            this.cacheSizeMsg = Integer.parseInt(properties.getProperty("cacheSizeMsg"));
            this.counter = new AtomicInteger(0);
        } catch (
            IOException e) {
        }
    }
}
