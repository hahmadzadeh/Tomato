package GHS;

import java.util.Random;

public class Main {
    public static void main(String[] args) {
        int n = 13;
        int m = 13;
        Random random = new Random();
        int[] arr1 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 6, 5, 11 };
        int[] arr2 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12 };
        double[] arr3 = { 4, 2, 4, 6, 7, 16, 7, 5, 3, 5, 10, 10, 4 };
        for (int i = 0; i < m; i++) {
            // EdgeHandler.getInstance().addEdge(random.nextInt(n), random.nextInt(n),
            // random.nextDouble() * 100);
            EdgeHandler.getInstance().addEdge(arr1[i], arr2[i], arr3[i]);
        }

        for (int i = 0; i < n; i++) {
            Node node = NodeHandler.getNodeHandler().constructNode(i);
            // System.out.println(node);
        }
        for (int i = 0; i < n; i++) {
            Node node = NodeHandler.getNodeHandler().getNodeById(i);
            Thread tr = new Thread(node);
            tr.start();
        }

        Thread tr = new Thread(NodeHandler.getNodeHandler());
        tr.start();
    }
}