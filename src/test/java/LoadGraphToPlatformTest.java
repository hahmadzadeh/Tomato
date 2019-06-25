import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import loader.LoadGraphToPlatform;
import org.junit.Test;

public class LoadGraphToPlatformTest {

    @Test
    public void loadSimple4Node4Edge() {
        try {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform();
            loadGraphToPlatform.initialLoadFromTextFile("/input2");
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void loadModerate1000Edge100Node() {
        try {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform();
            loadGraphToPlatform.initialLoadFromTextFile("/test");
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void loadLargeGraph() {
        try {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform();
            loadGraphToPlatform.initialLoadFromTextFile("/bio-mouse-gene/bio-mouse-gene.edges");
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

}
