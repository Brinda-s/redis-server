import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, List<String>> listStore = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, LinkedList<Object>> waiters = new ConcurrentHashMap<>();
  // Stream store: key → ordered list of entries
  static ConcurrentHashMap<String, List<StreamEntry>> streamStore = new ConcurrentHashMap<>();

  // A stream entry: an ID + key-value pairs
  static class StreamEntry {
    String id;
    LinkedHashMap<String, String> fields = new LinkedHashMap<>();
    StreamEntry(String id) { this.id = id; }
  }

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
              out.write("+PONG\r\n".getBytes()); break;

            case "ECHO":
              String msg = parts[1];
              out.write(("$" + msg.length() + "\r\n" + msg + "\r\n").getBytes()); break;

            case "SET":
              store.put(parts[1], parts[2]);
              if (parts.length >= 5 && parts[3].toUpperCase().equals("PX")) {
                expiry.put(parts[1], System.currentTimeMillis() + Long.parseLong(parts[4]));
              } else {
                expiry.remove(parts[1]);
              }
              out.write("+OK\r\n".getBytes()); break;

            case "GET": {
              String k = parts[1];
              if (expiry.containsKey(k) && System.currentTimeMillis() > expiry.get(k)) {
                store.remove(k); expiry.remove(k);
                out.write("$-1\r\n".getBytes());
              } else {
                String v = store.get(k);
                out.write(v == null ? "$-1\r\n".getBytes()
                  : ("$" + v.length() + "\r\n" + v + "\r\n").getBytes());
              }
              break;
            }

            case "TYPE": {
              String k = parts[1];
              if (store.containsKey(k))       out.write("+string\r\n".getBytes());
              else if (listStore.containsKey(k))  out.write("+list\r\n".getBytes());
              else if (streamStore.containsKey(k)) out.write("+stream\r\n".getBytes());
              else                             out.write("+none\r\n".getBytes());
              break;
            }

            case "XADD": {
              String sKey = parts[1];
              String id   = parts[2];

              // Full auto-generate: replace * with currentTimeMs-* BEFORE any parsing
              if (id.equals("*")) {
                id = System.currentTimeMillis() + "-*";
              }

              String[] idParts = id.split("-");
              long newMs  = Long.parseLong(idParts[0]);
              long newSeq;

              List<StreamEntry> stream = streamStore.computeIfAbsent(sKey, k -> new ArrayList<>());

              if (idParts[1].equals("*")) {
                // Auto-generate sequence number
                if (!stream.isEmpty()) {
                  StreamEntry last = stream.get(stream.size() - 1);
                  String[] lastParts = last.id.split("-");
                  long lastMs  = Long.parseLong(lastParts[0]);
                  long lastSeq = Long.parseLong(lastParts[1]);
                  if (newMs == lastMs) {
                    newSeq = lastSeq + 1; // same ms → increment seq
                  } else {
                    newSeq = (newMs == 0) ? 1 : 0; // new ms → start at 0 (or 1 if ms=0)
                  }
                } else {
                  newSeq = (newMs == 0) ? 1 : 0; // empty stream
                }
                id = newMs + "-" + newSeq;
              } else {
                newSeq = Long.parseLong(idParts[1]);
              }

              // 0-0 is always invalid
              if (newMs == 0 && newSeq == 0) {
                out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes());
                break;
              }

              // Validate against last entry
              if (!stream.isEmpty()) {
                StreamEntry last = stream.get(stream.size() - 1);
                String[] lastParts = last.id.split("-");
                long lastMs  = Long.parseLong(lastParts[0]);
                long lastSeq = Long.parseLong(lastParts[1]);
                if (newMs < lastMs || (newMs == lastMs && newSeq <= lastSeq)) {
                  out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                  break;
                }
              }

              StreamEntry entry = new StreamEntry(id);
              for (int i = 3; i + 1 < parts.length; i += 2) entry.fields.put(parts[i], parts[i + 1]);
              stream.add(entry);
              out.write(("$" + id.length() + "\r\n" + id + "\r\n").getBytes());
              break;
            }

            case "XRANGE": {
              String sKey  = parts[1];
              String start = parts[2]; // e.g. "0-1" or "0"
              String end   = parts[3]; // e.g. "0-3" or "0"

              // Normalise: add default seq if missing
              long startMs  = Long.parseLong(start.contains("-") ? start.split("-")[0] : start);
              long startSeq = start.contains("-") ? Long.parseLong(start.split("-")[1]) : 0;
              long endMs    = Long.parseLong(end.contains("-") ? end.split("-")[0] : end);
              long endSeq   = end.contains("-") ? Long.parseLong(end.split("-")[1]) : Long.MAX_VALUE;

              List<StreamEntry> stream = streamStore.get(sKey);
              List<StreamEntry> results = new ArrayList<>();
              if (stream != null) {
                for (StreamEntry e : stream) {
                  String[] ep = e.id.split("-");
                  long eMs = Long.parseLong(ep[0]), eSeq = Long.parseLong(ep[1]);
                  boolean afterStart = eMs > startMs || (eMs == startMs && eSeq >= startSeq);
                  boolean beforeEnd  = eMs < endMs   || (eMs == endMs   && eSeq <= endSeq);
                  if (afterStart && beforeEnd) results.add(e);
                }
              }

              out.write(encodeEntries(results).getBytes());
              break;
            }

            case "BLPOP": {
              String bKey = parts[1];
              double timeoutSecs = Double.parseDouble(parts[2]);
              long timeoutMs = (long)(timeoutSecs * 1000);
              long deadline = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;
              listStore.putIfAbsent(bKey, new ArrayList<>());
              List<String> bList = listStore.get(bKey);
              Object lock = new Object();
              waiters.computeIfAbsent(bKey, k -> new LinkedList<>()).add(lock);
              String popped = null;
              synchronized (lock) {
                while (true) {
                  synchronized (bList) {
                    if (!bList.isEmpty()) {
                      LinkedList<Object> q = waiters.get(bKey);
                      if (q != null && q.peek() == lock) { q.poll(); popped = bList.remove(0); }
                    }
                  }
                  if (popped != null) break;
                  long remaining = deadline - System.currentTimeMillis();
                  if (remaining <= 0) {
                    LinkedList<Object> q = waiters.get(bKey);
                    if (q != null) q.remove(lock);
                    break;
                  }
                  lock.wait(remaining);
                }
              }
              if (popped == null) {
                out.write("*-1\r\n".getBytes());
              } else {
                out.write(("*2\r\n$" + bKey.length() + "\r\n" + bKey + "\r\n"
                         + "$" + popped.length() + "\r\n" + popped + "\r\n").getBytes());
              }
              break;
            }

            case "LLEN": {
              List<String> l = listStore.get(parts[1]);
              out.write((":" + (l == null ? 0 : l.size()) + "\r\n").getBytes()); break;
            }

            case "LPOP": {
              List<String> pl = listStore.get(parts[1]);
              if (pl == null || pl.isEmpty()) {
                out.write("$-1\r\n".getBytes());
              } else {
                synchronized (pl) {
                  if (parts.length == 2) {
                    String p = pl.remove(0);
                    out.write(("$" + p.length() + "\r\n" + p + "\r\n").getBytes());
                  } else {
                    int count = Math.min(Integer.parseInt(parts[2]), pl.size());
                    StringBuilder sb = new StringBuilder("*" + count + "\r\n");
                    for (int i = 0; i < count; i++) {
                      String el = pl.remove(0);
                      sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n");
                    }
                    out.write(sb.toString().getBytes());
                  }
                }
              }
              break;
            }

            case "LPUSH": {
              String k = parts[1];
              listStore.putIfAbsent(k, new ArrayList<>());
              List<String> l = listStore.get(k);
              synchronized (l) {
                for (int i = 2; i < parts.length; i++) l.add(0, parts[i]);
                out.write((":" + l.size() + "\r\n").getBytes());
              }
              notifyWaiter(k); break;
            }

            case "RPUSH": {
              String k = parts[1];
              listStore.putIfAbsent(k, new ArrayList<>());
              List<String> l = listStore.get(k);
              synchronized (l) {
                for (int i = 2; i < parts.length; i++) l.add(parts[i]);
                out.write((":" + l.size() + "\r\n").getBytes());
              }
              notifyWaiter(k); break;
            }

            case "LRANGE": {
              String k = parts[1];
              int start = Integer.parseInt(parts[2]);
              int stop  = Integer.parseInt(parts[3]);
              List<String> l = listStore.get(k);
              if (l == null) {
                out.write("*0\r\n".getBytes());
              } else {
                synchronized (l) {
                  int size = l.size();
                  if (start < 0) start = Math.max(0, size + start);
                  if (stop  < 0) stop  = size + stop;
                  if (start >= size || start > stop) {
                    out.write("*0\r\n".getBytes());
                  } else {
                    stop = Math.min(stop, size - 1);
                    List<String> slice = l.subList(start, stop + 1);
                    StringBuilder sb = new StringBuilder("*" + slice.size() + "\r\n");
                    for (String el : slice)
                      sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n");
                    out.write(sb.toString().getBytes());
                  }
                }
              }
              break;
            }
          }
          out.flush();
        }
      }
    } catch (IOException | InterruptedException e) {
      System.out.println("Exception: " + e.getMessage());
    } finally {
      try { clientSocket.close(); } catch (IOException e) { System.out.println("IOException: " + e.getMessage()); }
    }
  }

  // Encode a list of stream entries as a nested RESP array
  private static String encodeEntries(List<StreamEntry> entries) {
    StringBuilder sb = new StringBuilder("*" + entries.size() + "\r\n");
    for (StreamEntry e : entries) {
      sb.append("*2\r\n");
      sb.append("$").append(e.id.length()).append("\r\n").append(e.id).append("\r\n");
      sb.append("*").append(e.fields.size() * 2).append("\r\n");
      for (Map.Entry<String, String> f : e.fields.entrySet()) {
        sb.append("$").append(f.getKey().length()).append("\r\n").append(f.getKey()).append("\r\n");
        sb.append("$").append(f.getValue().length()).append("\r\n").append(f.getValue()).append("\r\n");
      }
    }
    return sb.toString();
  }

  private static void notifyWaiter(String key) {
    LinkedList<Object> q = waiters.get(key);
    if (q != null) {
      synchronized (q) {
        Object first = q.peek();
        if (first != null) { synchronized (first) { first.notify(); } }
      }
    }
  }
}