import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  // Shared store across all clients
  static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

    int port = 6379;
    try {
      ServerSocket serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static void handleClient(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      OutputStream out = clientSocket.getOutputStream();

      String line;
      while ((line = in.readLine()) != null) {
        if (line.startsWith("*")) {
          int numArgs = Integer.parseInt(line.substring(1));
          String[] parts = new String[numArgs];

          for (int i = 0; i < numArgs; i++) {
            in.readLine();             // skip $<length>
            parts[i] = in.readLine(); // read value
          }

          String command = parts[0].toUpperCase();

          switch (command) {
            case "PING":
              out.write("+PONG\r\n".getBytes());
              break;

            case "ECHO":
              String msg = parts[1];
              out.write(("$" + msg.length() + "\r\n" + msg + "\r\n").getBytes());
              break;

            case "SET":
              store.put(parts[1], parts[2]);
              out.write("+OK\r\n".getBytes());
              break;

            case "GET":
              String val = store.get(parts[1]);
              if (val == null) {
                out.write("$-1\r\n".getBytes());
              } else {
                out.write(("$" + val.length() + "\r\n" + val + "\r\n").getBytes());
              }
              break;
          }
          out.flush();
        }
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        clientSocket.close();
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}