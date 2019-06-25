package loader;

import GHS.Edge;
import GHS.Neighbour;
import GHS.Node;
import cache.MessageCacheQueue;
import cache.NeighbourCache;
import cache.NodeCache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


public class LoadGraphToPlatform {

    private Properties properties;
    private NodeCache nodeCache;
    private NeighbourCache neighbourCache;

    public LoadGraphToPlatform() {
        this.properties = new Properties();
        try (InputStream in = LoadGraphToPlatform.class.getResourceAsStream("/config.properties")) {
            System.out.println(in);
            properties.load(in);
            nodeCache = new NodeCache();
            neighbourCache = new NeighbourCache();
        } catch (IOException e) {
        }
    }

    public void initialLoadFromTextFile(String filename) throws IOException, URISyntaxException {
        InputStream resourceAsStream = LoadGraphToPlatform.class.getResourceAsStream(filename);
        BufferedReader buff = new BufferedReader(new InputStreamReader(resourceAsStream));
        String e;
        while ((e = buff.readLine()) != null) {
//        Files.lines(Paths.get(LoadGraphToPlatform.class.getResource(filename).toURI()))
//                .parallel().forEach(e -> {
            String[] line = e.split(" ");
            if(line[0].equals(line[1])){
                continue;
            }
            if (!nodeCache.exist(Integer.parseInt(line[0]))) {
                Node node = Node.build_NodeTemplate(line[0]);
                nodeCache.addNode(node);
            }
            if (!nodeCache.exist(Integer.parseInt(line[1]))) {
                Node node = Node.build_NodeTemplate(line[1]);
                nodeCache.addNode(node);
            }
            Neighbour neighbour = new Neighbour(Integer.parseInt(line[0]), Integer.parseInt(line[1]), Double.parseDouble(line[2]), Neighbour.BASIC);
            Neighbour neighbour1 = new Neighbour(Integer.parseInt(line[1]), Integer.parseInt(line[0]), Double.parseDouble(line[2]), Neighbour.BASIC);
            neighbourCache.addNeighbour(neighbour);
            neighbourCache.addNeighbour(neighbour1);
        }
        nodeCache.counter.set(0);
        neighbourCache.counter.set(0);
        neighbourCache.flush("edge%%", Neighbour.class, neighbourCache.edgeRepository);
        nodeCache.flush("node%%", Node.class, nodeCache.nodeRepository);
        try (Jedis jedis = MessageCacheQueue.jedisPool.getResource()) {
            Set<String> keys = jedis.keys("cc%%*");
            for (String key : keys) {
                jedis.del(key);
            }
        }
    }
}
