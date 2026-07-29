package raft;

import java.util.List;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        // Initialize a 3-node cluster
        Node node1 = new Node(1);
        Node node2 = new Node(2);
        Node node3 = new Node(3);

        node1.setPeers(List.of(node2, node3));
        node2.setPeers(List.of(node1, node3));
        node3.setPeers(List.of(node1, node2));

        System.out.println(">>> Waiting for a leader to be elected...");
        Thread.sleep(2000);

        // Locate the elected leader
        Node leader = null;
        List<Node> cluster = List.of(node1, node2, node3);

        for (Node node : cluster) {
            if (node.getState() == NodeState.LEADER) {
                leader = node;
                break;
            }
        }

        // Submit a client request
        if (leader != null) {
            System.out.println("\n--- STEP 1: SUBMITTING CLIENT DATA ---");

            leader.executeClientRequest("PUT user=alice");

            Thread.sleep(500);

            System.out.println("\n--- CLUSTER STATE AFTER WRITE ---");

            node1.printStatus();
            node2.printStatus();
            node3.printStatus();
        } else {
            System.out.println("No active leader found yet.");
        }

        System.exit(0);
    }
}