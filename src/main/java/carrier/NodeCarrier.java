package carrier;

import GHS.Node;
import cache.MessageQueue;
import cache.NodeCache;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class NodeCarrier implements Callable<List<Node>> {
    private ExecutorService executorService;
    private LinkedBlockingQueue<String> linkedBlockingQueue;
    private NodeCache nodeCache;
    private MessageQueue messageQueue;

    private int cacheSize;
    private List<Future<Node>> slavesResult;
    private List<Node> result;

    public NodeCarrier(LinkedBlockingQueue<String> linkedBlockingQueue, NodeCache nodeCache
            , int num, MessageQueue messageQueue) {
        this.executorService = Executors.newFixedThreadPool(20);
        this.linkedBlockingQueue = linkedBlockingQueue;
        this.nodeCache = nodeCache;
        this.cacheSize = num;
        this.slavesResult = new LinkedList<>();
        this.messageQueue = messageQueue;
        this.result = new LinkedList<>();
    }

    @Override
    public List<Node> call() throws ExecutionException, InterruptedException {
        result.clear();
        while (true) {
            for (int i = 0; i < cacheSize; i++) {
                String poll = linkedBlockingQueue.poll();
                if (poll == null) {
                    for (Future<Node> future : slavesResult) {
                        result.add(future.get());
                    }
                    slavesResult.clear();
                    return result;
                }
                Node node = nodeCache.getNode(poll);
                node.msgQueue = messageQueue;
                slavesResult.add(executorService.submit(node));
            }
        }
    }
}
