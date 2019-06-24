package loader;

import GHS.Node;
import cache.NodeCache;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class LoadGraphToPlatform {

    private Properties properties;
    private NodeCache nodeCache;

    public LoadGraphToPlatform() {
        this.properties = new Properties();
        try (InputStream in = LoadGraphToPlatform.class.getResourceAsStream("/config.properties")) {
            System.out.println(in);
            properties.load(in);
            nodeCache = new NodeCache();
        } catch (IOException e) {
        }
    }

    public void loadFromTextFile(String filename) throws IOException, URISyntaxException {
        AtomicInteger counter = new AtomicInteger();
        Files.lines(Paths.get(LoadGraphToPlatform.class.getResource(filename).toURI()))
            .parallel().forEach(e -> {
                String[] line = e.split(" ");
                if (nodeCache.getNode(Integer.parseInt(line[0])) == null){
                    Node node = Node.build_NodeTemplate(line[0]);
                    nodeCache.addNode(node);
                }else if(nodeCache.getNode(Integer.parseInt(line[1])) == null){

                }
            });
        //System.out.println(count + " -- " + properties.getProperty("batchSize"));
    }
}
