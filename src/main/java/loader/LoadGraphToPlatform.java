package loader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;


public class LoadGraphToPlatform {
    private Properties properties;

    public LoadGraphToPlatform() {
        this.properties = new Properties();
        try(InputStream in = LoadGraphToPlatform.class.getResourceAsStream("/config.properties")){
            System.out.println(in);
            properties.load(in);
        } catch (IOException e) {
        }
    }

    public  void loadFromTextFile(String filename) throws IOException, URISyntaxException {
        long count = Files.lines(Paths.get(LoadGraphToPlatform.class.getResource(filename).toURI())).count();
        System.out.println(count + " -- " + properties.getProperty("batchSize"));
    }
}
