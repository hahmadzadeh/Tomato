package GHS;

import cache.MessageCacheQueue;
import cache.MessageQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        int n = 3;
        int m = 3;
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        int[] arr1 = { 0, 1, 2 };
        int[] arr2 = { 1, 2, 0 };
        double[] arr3 = { 12, 8, 6 };
        for (int i = 0; i < m; i++) {
            // EdgeHandler.getInstance().addEdge(random.nextInt(n), random.nextInt(n),
            // random.nextDouble() * 100);
            EdgeHandler.getInstance().addEdge(arr1[i], arr2[i], arr3[i]);
        }
        MessageQueue queue = new MessageCacheQueue();
        for (int i = 0; i < n; i++) {
            Node node = NodeHandler.getNodeHandler().constructNode(i, queue);
            System.out.println(node);
        }
        for (int i = 0; i < n; i++) {
            Node node = NodeHandler.getNodeHandler().getNodeById(i);
            executorService.submit(node);
        }
        System.out.println("salam");
        Thread.sleep(7000);
        System.out.println("hi");
    }
}