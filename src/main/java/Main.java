import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
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
            in.readLine();              // read $<length> line, ignore it
            parts[i] = in.readLine();  // read the actual value
          }

          String command = parts[0].toUpperCase();

          if (command.equals("PING")) {
            out.write("+PONG\r\n".getBytes());
          } else if (command.equals("ECHO")) {
            String msg = parts[1];
            out.write(("$" + msg.length() + "\r\n" + msg + "\r\n").getBytes());
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