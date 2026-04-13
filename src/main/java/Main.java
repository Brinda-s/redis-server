import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>(); // key → expiry time in ms

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
            in.readLine();
            parts[i] = in.readLine();
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
              // Check for PX option: SET key value PX 100
              if (parts.length >= 5 && parts[3].toUpperCase().equals("PX")) {
                long ttl = Long.parseLong(parts[4]);
                expiry.put(parts[1], System.currentTimeMillis() + ttl);
              } else {
                expiry.remove(parts[1]); // clear any previous expiry
              }
              out.write("+OK\r\n".getBytes());
              break;

            case "GET":
              String key = parts[1];
              // Check if key is expired
              if (expiry.containsKey(key) && System.currentTimeMillis() > expiry.get(key)) {
                store.remove(key);
                expiry.remove(key);
                out.write("$-1\r\n".getBytes());
              } else {
                String val = store.get(key);
                if (val == null) {
                  out.write("$-1\r\n".getBytes());
                } else {
                  out.write(("$" + val.length() + "\r\n" + val + "\r\n").getBytes());
                }
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