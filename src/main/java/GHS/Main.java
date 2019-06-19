package GHS;

import java.util.Random;

public class Main {
    public static void main(String[] args) {
        int n = 3;
        int m = 3;
        Random random = new Random();
        int[] arr1 = { 0, 1, 2 };
        int[] arr2 = { 1, 2, 0 };
        double[] arr3 = { 12, 8, 6 };
        for (int i = 0; i < m; i++) {
            // EdgeHandler.getInstance().addEdge(random.nextInt(n), random.nextInt(n),
            // random.nextDouble() * 100);
            EdgeHandler.getInstance().addEdge(arr1[i], arr2[i], arr3[i]);
        }

        for (int i = 0; i < n; i++) {
            Node node = NodeHandler.getNodeHandler().constructNode(i);
            System.out.println(node);
        }
        for (int i = 0; i < n; i++) {
            Node node = NodeHandler.getNodeHandler().getNodeById(i);
            Thread tr = new Thread(node);
            tr.start();
        }

        Thread tr = new Thread(NodeHandler.getNodeHandler());
        tr.start();

        System.out.println("salam");
    }
}