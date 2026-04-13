import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, List<String>> listStore = new ConcurrentHashMap<>(); // key → list of values

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
              if (parts.length >= 5 && parts[3].toUpperCase().equals("PX")) {
                long ttl = Long.parseLong(parts[4]);
                expiry.put(parts[1], System.currentTimeMillis() + ttl);
              } else {
                expiry.remove(parts[1]);
              }
              out.write("+OK\r\n".getBytes());
              break;

            case "GET":
              String key = parts[1];
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

            case "LRANGE":
              String lKey = parts[1];
              int start = Integer.parseInt(parts[2]);
              int stop  = Integer.parseInt(parts[3]);
              List<String> lList = listStore.get(lKey);
              if (lList == null) {
                out.write("*0\r\n".getBytes()); // empty array
              } else {
                synchronized (lList) {
                  int size = lList.size();
                  // convert negative indexes to positive
                  if (start < 0) start = Math.max(0, size + start);
                  if (stop  < 0) stop  = size + stop;
                  if (start >= size || start > stop) {
                    out.write("*0\r\n".getBytes());
                  } else {
                    stop = Math.min(stop, size - 1); // clamp stop to last index
                    List<String> slice = lList.subList(start, stop + 1);
                    StringBuilder sb = new StringBuilder();
                    sb.append("*").append(slice.size()).append("\r\n");
                    for (String el : slice) {
                      sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n");
                    }
                    out.write(sb.toString().getBytes());
                  }
                }
              }
              break;

            case "LPUSH":
              String lpKey = parts[1];
              listStore.putIfAbsent(lpKey, new ArrayList<>());
              List<String> lpList = listStore.get(lpKey);
              synchronized (lpList) {
                // insert each element at index 0 (reverse order)
                for (int i = 2; i < parts.length; i++) {
                  lpList.add(0, parts[i]);
                }
                out.write((":" + lpList.size() + "\r\n").getBytes());
              }
              break;

            case "RPUSH":
              // parts[1] = key, parts[2..] = values to append
              String listKey = parts[1];
              listStore.putIfAbsent(listKey, new ArrayList<>());
              List<String> list = listStore.get(listKey);
              synchronized (list) {
                for (int i = 2; i < parts.length; i++) {
                  list.add(parts[i]);
                }
                out.write((":" + list.size() + "\r\n").getBytes()); // RESP integer
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