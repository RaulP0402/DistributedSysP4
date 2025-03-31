
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

public class Server {
    
    String id, bootstrapIP;
    Socket socket;
    int portNumber, bootstrapPortNumber;
    private volatile DataInputStream bootstrapDataIn;
    private volatile DataOutputStream bootstrapDataOut;
    HashMap<String, String> data;

    Server(){
        this.id = null;
        this.portNumber = -1;
        this.bootstrapIP = null;
        this.bootstrapPortNumber = -1;
        this.bootstrapDataIn = null;
        this.bootstrapDataOut = null;
        this.data = new HashMap<>();
    }

    /*
     * ---------------------------------------- HELPER FUNCTIONS ----------------------------------------
     */

    public void parseConfigFile(String configFile) {
        File file = new File(configFile);

        try (Scanner fileScanner = new Scanner( new FileReader(file)) ) {
             
            this.id = fileScanner.nextLine();
            this.portNumber = Integer.parseInt(fileScanner.nextLine());

            String[] bootstrapIPAndPort = fileScanner.nextLine().split(" ");
            this.bootstrapIP = bootstrapIPAndPort[0];
            this.bootstrapPortNumber = Integer.parseInt(bootstrapIPAndPort[1]);


        } catch (IOException e) {
            System.out.println("ERROR reading config file. " + e.toString());
            exit();
        }
    }


    protected void printDataState() {
        System.out.println("Key:    Val:");
        for (String key: this.data.keySet()) {
            System.out.println(key + "  " + this.data.get(key));
        }
        System.out.println("");
    }

    /*
     * ---------------------------------------- COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */

    /*
     * commands listener function listens and handles commands sent by the bootstrap node.
     * 
     * Possible commands include:
     * - put key_val: put the keyVal string into the hashmap
     * - send <start> <end> <receiverIP> <receiverPort>: Send any values you may have in range [start, end] to the server with receiverIP and receiverPort
     * - connectToServerAndPut : Use your portNumber to listen for a socket, connect, and put all the keyVal's it sends you.
     */
    public void commandsListener() {
        new Thread ( () -> commandLineInputListener() ).start();

        while (true) {
            try {
                if (this.bootstrapDataIn != null) {
                    String command = this.bootstrapDataIn.readUTF();
                    String[] commandSplit = command.split(" ", 2);

                    switch(commandSplit[0]) {
                        case("put") -> put(commandSplit[1]);
                        case("sendDataToServer") -> sendDataToServer(commandSplit[1]);
                        case("connectToServerAndPut") -> connectToServerAndPut();
                        case("sendToBootstrap") -> sendToBootstrap(commandSplit[1]);
                        default -> System.out.println(command);
                    }
                    
                } 

            } catch (Exception e) {
            }
        }
    }

    private void put(String keyAndVal) {
        String[] keyAndValSplit = keyAndVal.split(" ");
        String key = keyAndValSplit[0], val = keyAndValSplit[1];

        this.data.put(key, val);
        
    }

    private void sendDataToServer(String information) {
        String[] informationSplit = information.split(" ");
        int start = Integer.parseInt(informationSplit[0]);
        int end = Integer.parseInt(informationSplit[1]);
        String receiverIP = informationSplit[2];
        int receiverPort = Integer.parseInt(informationSplit[3]);

        // Connect to receiver
        try (Socket senderSocket = new Socket(receiverIP, receiverPort)){
            senderSocket.setReuseAddress(true);

            DataOutputStream dOut = new DataOutputStream(senderSocket.getOutputStream());

            for (int i = start; i <= end; i++) {
                String key = String.valueOf(i);
                if (this.data.containsKey(key)) {
                    String message = key + " " + this.data.get(key);
                    dOut.writeUTF(message);
                    dOut.flush();
                    this.data.remove(key);
                }
            }
            
            dOut.writeUTF("EOF");
            dOut.flush();
            
        } catch (Exception e) {
            System.out.println("ERROR in send: " + e.toString());
        }    }

    private void connectToServerAndPut() {
        try (ServerSocket serverSocket = new ServerSocket(this.portNumber)) {
            serverSocket.setReuseAddress(true);
            this.bootstrapDataOut.writeUTF("READY");
            this.bootstrapDataOut.flush();
            
            Socket receiverSocket = serverSocket.accept();
            receiverSocket.setReuseAddress(true);
            DataInputStream dIn = new DataInputStream(receiverSocket.getInputStream());

            String keyAndVal = dIn.readUTF();
            while (!keyAndVal.equals("EOF")) {
                put(keyAndVal);
                keyAndVal = dIn.readUTF();
            }
        } catch (IOException e) {
            System.out.println("ERROR in connectToServerAndPut: " + e.toString());
        }
    }

    private void sendToBootstrap(String ranges) {
        String[] rangesSplit = ranges.split(" ");
        int start = Integer.parseInt(rangesSplit[0]), end = Integer.parseInt(rangesSplit[1]);

        // System.out.println("[DEBUG] Sending data to bootstrap");

        try {
            for (int i = start; i <= end; i++) {
                String key = String.valueOf(i);
                if (this.data.containsKey(key)) {
                    String message = key + " " + this.data.get(key);
                    this.bootstrapDataOut.writeUTF(message);
                    this.bootstrapDataOut.flush();
                    this.data.remove(key);
                }
            }

            this.bootstrapDataOut.writeUTF("EOF");
            this.bootstrapDataOut.flush();
        } catch (IOException e) {
            System.out.println("ERROR in send to bootstrap: " + e.toString());
        }

        // System.out.println("Finished sending data to bootstrap");
    }


    /*
     * ---------------------------------------- END OF COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */



    /*
     * ---------------------------------------- COMMAND LINE COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */
    /*
     * CommandLineInputListener function listens and handles all command line commands.
     * 
     * Possible commands include:
     * - enter
     * - exit
     */
    public void commandLineInputListener() {
        try (Scanner scanner = new Scanner(System.in)) {
            String input;
            while (true) {
                System.out.print("server> ");
                input = scanner.nextLine();

                String command = input.split(" ", 2)[0];
                switch (command) {
                    case("enter") -> enter();
                    case("exit") -> exit();
                    case("printData") -> printDataState();
                    default -> System.out.println("INVALID COMMAND");
                }

            }
        }
    }

    private void exit() {
        try (Socket exitSocket = new Socket(bootstrapIP, bootstrapPortNumber)){
            DataOutputStream exitDataOut = new DataOutputStream(exitSocket.getOutputStream());

            // Tell boostrap you want to exit system
            exitDataOut.writeUTF("exit " + this.id);
            exitDataOut.flush();

            // Wait for awknoledgement from Boostrap
            this.bootstrapDataIn.readUTF();

            // Close socket connection with bootstrap and data input/output stream
            this.socket.close();
            this.socket = null;
            this.bootstrapDataIn = null;
            this.bootstrapDataOut = null;
        } catch (Exception e) {
        }

    }

    private void enter() {
        try  {
            this.socket = new Socket(bootstrapIP, bootstrapPortNumber);
            this.bootstrapDataIn = new DataInputStream(socket.getInputStream());
            this.bootstrapDataOut = new DataOutputStream(socket.getOutputStream());
            System.out.println("Connected socket");
            
            // Let Bootstrap node know we want to enter the system
            this.bootstrapDataOut.writeUTF("enter " + this.id + " " + portNumber);
            this.bootstrapDataOut.flush();

        } catch (IOException e) {
            System.out.println("FAILED TO ENTER. " + e.toString());
        }
    }

     /*
     * ---------------------------------------- END OF COMMAND LINE COMMANDS LISTENER FUNCTIONS ----------------------------------------
     */

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Server <config_file_name>");
            return;
        }

        Server server = new Server();
        server.parseConfigFile(args[0]);
        server.commandsListener();
    }

}
