import cache.NeighbourCache;
import cache.NodeCache;
import java.io.IOException;
import loader.LoadGraphToPlatform;
import org.junit.Test;
import repository.EdgeRepository;
import repository.NodeRepository;

public class LoadGraphToPlatformTest {

    @Test
    public void loadSimple4Node4Edge() {
        try {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform(new NodeCache(new NodeRepository()), new NeighbourCache(new EdgeRepository()));
            loadGraphToPlatform.initialLoadFromTextFile("/input2");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void loadModerate1000Edge100Node() {
        try {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform(new NodeCache(new NodeRepository()), new NeighbourCache(new EdgeRepository()));
            loadGraphToPlatform.initialLoadFromTextFile("/test");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void loadLargeGraph() {
        try {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform(new NodeCache(new NodeRepository()), new NeighbourCache(new EdgeRepository()));
            loadGraphToPlatform.initialLoadFromTextFile("/bio-mouse-gene/bio-mouse-gene.edges");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
