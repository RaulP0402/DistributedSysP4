import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import static java.lang.System.exit;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class Bootstrap extends Server {

    String id;
    int portNumber;
    Node head;

    /*
     * ---------------------------------------- HELPER FUNCTIONS ----------------------------------------
     */

    private void initialize() {
        // Dummy previous and head, initial state of linked list is
        // dummyStart -> head -> dummyNext
        Node dummyHead = new Node("-1", -1, -1, -1);
        Node dummyTail = new Node("1024", -1, 1024, 1024);
        Node bootstrapNode = new Node(this.id, this.portNumber, 0, 1023);

        dummyHead.setNext(bootstrapNode);
        bootstrapNode.setPrevious(dummyHead);
        bootstrapNode.setNext(dummyTail);
        dummyTail.setPrevious(bootstrapNode);

        this.head = dummyHead;
    }

    @Override
    public void parseConfigFile(String configFile) {
        File file = new File(configFile);
        try (Scanner fileScanner = new Scanner( new FileReader(file) )){

            this.id = fileScanner.nextLine();
            this.portNumber = Integer.parseInt(fileScanner.nextLine());

            String keyVal = "";
            while (fileScanner.hasNext()) {
                // Process initial key, vals
                keyVal = fileScanner.nextLine();
                String[] keyValSplit = keyVal.split(" ");
                data.put(keyValSplit[0], keyValSplit[1]);
            }

        } catch (IOException e) {
            System.out.println("ERROR reading config file. " + e.toString());
            exit(1);
        }
    }
    

    private void seeNodePath() {
        System.out.println("\nPath of nodes");
        Node curNode = this.head.next;
        while (curNode != null && !curNode.id.equals("1024")) {
            System.out.println("Node: " + curNode.id + ", Start: " + curNode.start + ", End: " + curNode.end);
            curNode = curNode.next;
        }
        System.out.println("");
    }

    /*
     * ---------------------------------------- COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */

    /*
     * Commands Listener function listens for new commands from servers. 
     * The following are possible commands:
     * - enter <server_id>
     * - exit <server_id>
     */
 
    @Override
    public void commandsListener() {
        System.out.println("Starting Bootstrap server.");
        new Thread( () -> commandLineInputListener() ).start();

        try (ServerSocket serverSocket = new ServerSocket(this.portNumber)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket server = serverSocket.accept();
                DataInputStream dataIn = new DataInputStream(server.getInputStream());
                DataOutputStream dataOut = new DataOutputStream(server.getOutputStream());

                String command = dataIn.readUTF();            
                System.out.println("Server Command: " + command);

                /*  Command Options:
                *  - enter <server_id>
                *  - exit <server_id>
                */
                String[] commandSplit = command.split(" ", 3);
                switch(commandSplit[0]) {
                    case("enter") -> enterNewServer(commandSplit[1], commandSplit[2], dataIn, dataOut);
                    case("exit") -> exitServer(commandSplit[1]);
                }

            }
        } catch (IOException e) {
            System.out.println("ERROR: " + e.toString());
        }

    }

    private void exitServer(String id) {
        Node curNode = this.head;

        while (!curNode.id.equals(id)) {
            curNode = curNode.next;
        }

        Node previous = curNode.previous, next = curNode.next;

        // Update range of next node
        next.setStart(curNode.start);

        // Send data from current node to next node
        sendData(curNode, next);

        // Set link of previous <-> Next
        previous.setNext(next);
        next.setPrevious(previous);

        try {
            curNode.dataOut.writeUTF("FINISHED");
        } catch (IOException e) {
            System.out.println("EROOR in exit: " + e.toString());
        }
        // Remove curNode
        curNode = null;

    }

    private void enterNewServer(String id, String portNumber, DataInputStream dataIn, DataOutputStream dataOut) {
        Node curNode = this.head;

        // While id not in range of this node, continue onwards
        while (!curNode.inRange(id)) {
            curNode = curNode.next;
        }

        // CurNode is the one where this new server falls in
        Node newNode = new Node(
            id, 
            Integer.parseInt(portNumber), 
            curNode.start, 
            Integer.parseInt(id)
            );

        // set new range of curNode
        curNode.setStart(Integer.parseInt(id) + 1);

        // prevNode <-> newNOde
        curNode.previous.setNext(newNode);
        newNode.setPrevious(curNode.previous);

        // newNode <-> curNode
        curNode.setPrevious(newNode);
        newNode.setNext(curNode);

        // Set input/output stream of boostrap <-> newServer
        newNode.setDataIn(dataIn);
        newNode.setDataOut(dataOut);

        // Send any data from current node that new node needs
        sendData(curNode, newNode);
    }

    private void sendData(Node sender, Node receiver) {
        try {
            // If sender is bootstrap node
            if (sender.id.equals("0")) {
                for (int i = receiver.start; i <= receiver.end; i++) {
                    String key = String.valueOf(i);
                    if (data.containsKey(key)) {
                        String message = "put " + key + " " + data.get(key);
                        receiver.dataOut.writeUTF(message);
                        receiver.dataOut.flush();
                        data.remove(key);
                    }
                }

                // receiver.dataOut.writeUTF("EOF");
                // receiver.dataOut.flush();
            } else if (receiver.id.equals("0")) {
                
                String message = "sendToBootstrap " + receiver.start + " " + receiver.end + " ";
                sender.dataOut.writeUTF(message);
                sender.dataOut.flush();

                String newData = sender.dataIn.readUTF();
                while (!newData.equals("EOF")) {
                    String[] newDataSplit = newData.split(" ");
                    String key = newDataSplit[0], val = newDataSplit[1];
                    this.data.put(key, val);
                    newData = sender.dataIn.readUTF();
                } 

            } else {
                // Message for the receiver with information
                String msg = "connectToServerAndPut";
                receiver.dataOut.writeUTF(msg);
                receiver.dataOut.flush();

                // Get awkn that receiver is ready for connection
                receiver.dataIn.readUTF();

                // Message for the sender with information
                msg = "sendDataToServer " + receiver.start + " " + receiver.end + " " + receiver.ip + " " + receiver.portNumber;
                sender.dataOut.writeUTF(msg);
                sender.dataOut.flush();
            }

        } catch (IOException e) {
            System.out.println("Error Sending Information From Node: " + sender.id + " to Node " + receiver.id);
        }
    }

    /*
     * ---------------------------------------- END OF COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */




    /*
     * ---------------------------------------- COMMAND LINE COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */
    /*
     * CommandLineInputListener listens and handles any command line input command.
     * The following are possible commands:
     * - insert key val
     * - delete key
     * - lookup key
     */
    @Override
    public void commandLineInputListener() {
        try(Scanner scanner = new Scanner(System.in)) {
            String input;
            while (true) {
                System.out.print("bootstrap> ");
                input = scanner.nextLine();

                String[] command = input.split(" ", 2);
                switch (command[0]) {
                    case ("insert") -> insert();
                    case ("lookup") -> lookup();
                    case ("delete") -> delete();
                    case ("printData") -> printDataState();
                    case ("seeNodePath") -> seeNodePath();
                    default -> System.out.println("INVALID COMMAND");
                }
                
            }
        }
    }

    private void insert() {
        System.out.println("ERROR: Not yet implemented");
    }

    private void lookup() {
        System.out.println("ERROR: Not yet implemented");
    }

    private void delete() {
        System.out.println("ERROR: Not yet implemented");
    }

    /*
     * ---------------------------------------- END OF COMMAND LINE COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */


    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("usage: java Bootstrap <config_file_path> ");
            return;
        } 

        Bootstrap bootstrapServer = new Bootstrap();
        bootstrapServer.parseConfigFile(args[0]);
        bootstrapServer.initialize();
        bootstrapServer.commandsListener();
    }

}


class Node {
    String id, ip;
    int portNumber, start, end;
    Node next, previous;
    DataInputStream dataIn;
    DataOutputStream dataOut;

    Node(
        String id, 
        int portNumner,
        int start,
        int end
    ){
        this.id = id;
        this.ip = "127.0.0.1";
        this.portNumber = portNumner;
        this.start = start;
        this.end = end;
        this.next = null;
        this.previous = null;
    }

    public boolean inRange(String id) {
        int key = Integer.parseInt(id);
        return this.start <= key && this.end >= key;
    }

    public void setNext(Node node){
        this.next = node;
    }

    public void setPrevious(Node node){
        this.previous = node;
    }

    public void setDataIn(DataInputStream dIn){
        this.dataIn = dIn;
    }

    public void setDataOut(DataOutputStream dOut) {
        this.dataOut = dOut;
    }

    public void setStart(int newStart) {
        this.start = newStart;
    }

}
