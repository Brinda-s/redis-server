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
  static ConcurrentHashMap<String, List<StreamEntry>> streamStore = new ConcurrentHashMap<>();
  static String role = "master";
  static ConcurrentHashMap<String, LinkedList<Object>> streamWaiters = new ConcurrentHashMap<>();

  static class StreamEntry {
    String id;
    LinkedHashMap<String, String> fields = new LinkedHashMap<>();
    StreamEntry(String id) { this.id = id; }
  }

  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");
    // If replica, connect to master and send handshake
    String masterHost = null;
    int masterPort = -1;
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals("--replicaof")) {
        String[] hp = args[i + 1].split(" ");
        masterHost = hp[0];
        masterPort = Integer.parseInt(hp[1]);
      }
    }

    int port = 6379;
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);
      if (args[i].equals("--replicaof")) role = "slave";
    }

    // Connect to master and send handshake
    if (masterHost != null) {
      try {
        Socket masterSocket = new Socket(masterHost, masterPort);
        OutputStream masterOut = masterSocket.getOutputStream();
        BufferedReader masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

        // Step 1: PING
        masterOut.write("*1\r\n$4\r\nPING\r\n".getBytes());
        masterOut.flush();
        masterIn.readLine(); // read +PONG

        // Step 2a: REPLCONF listening-port <PORT>
        String portStr = String.valueOf(port);
        masterOut.write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"
            + portStr.length() + "\r\n" + portStr + "\r\n").getBytes());
        masterOut.flush();
        masterIn.readLine(); // read +OK

        // Step 2b: REPLCONF capa psync2
        masterOut.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
        masterOut.flush();
        masterIn.readLine(); // read +OK

        // Step 3: PSYNC ? -1
        masterOut.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
        masterOut.flush();
        masterIn.readLine(); // read +FULLRESYNC <id> 0

      } catch (IOException e) {
        System.out.println("Failed to connect to master: " + e.getMessage());
      }
    }
    try {
      ServerSocket serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }
    } catch (IOException e) { System.out.println("IOException: " + e.getMessage()); }
  }

  private static void handleClient(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      OutputStream out = clientSocket.getOutputStream();
      boolean inMulti = false;
      List<String[]> txQueue = new ArrayList<>();

      String line;
      while ((line = in.readLine()) != null) {
        if (!line.startsWith("*")) continue;
        int numArgs = Integer.parseInt(line.substring(1));
        String[] parts = new String[numArgs];
        for (int i = 0; i < numArgs; i++) { in.readLine(); parts[i] = in.readLine(); }
        String command = parts[0].toUpperCase();

        // Queue commands when inside MULTI (except EXEC/DISCARD themselves)
        if (inMulti && !command.equals("EXEC") && !command.equals("DISCARD")) {
          txQueue.add(parts);
          out.write("+QUEUED\r\n".getBytes());
          out.flush();
          continue;
        }

        // Handle transaction control commands directly
        if (command.equals("MULTI")) {
          inMulti = true;
          out.write("+OK\r\n".getBytes());
        } else if (command.equals("EXEC")) {
          if (!inMulti) {
            out.write("-ERR EXEC without MULTI\r\n".getBytes());
          } else {
            inMulti = false;
            StringBuilder sb = new StringBuilder("*" + txQueue.size() + "\r\n");
            for (String[] cmd : txQueue) sb.append(execCommand(cmd[0].toUpperCase(), cmd, out));
            txQueue.clear();
            out.write(sb.toString().getBytes());
          }
        } else if (command.equals("DISCARD")) {
          if (!inMulti) {
            out.write("-ERR DISCARD without MULTI\r\n".getBytes());
          } else {
            inMulti = false;
            txQueue.clear();
            out.write("+OK\r\n".getBytes());
          }
        } else {
          out.write(execCommand(command, parts, out).getBytes());
        }
        out.flush();
      }
    } catch (IOException | InterruptedException e) { System.out.println("Exception: " + e.getMessage()); }
    finally { try { clientSocket.close(); } catch (IOException e) { System.out.println("IOException: " + e.getMessage()); } }
  }

  // Executes a single command and returns the RESP response as a String
  private static String execCommand(String command, String[] parts, OutputStream out) throws InterruptedException, IOException {
    switch (command) {
      case "INFO": {
        String info = "role:" + role + "\r\n"
                    + "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
                    + "master_repl_offset:0";
        return "$" + info.length() + "\r\n" + info + "\r\n";
      }

      case "PING": return "+PONG\r\n";

      case "ECHO": return "$" + parts[1].length() + "\r\n" + parts[1] + "\r\n";

      case "SET":
        store.put(parts[1], parts[2]);
        if (parts.length >= 5 && parts[3].toUpperCase().equals("PX"))
          expiry.put(parts[1], System.currentTimeMillis() + Long.parseLong(parts[4]));
        else expiry.remove(parts[1]);
        return "+OK\r\n";

      case "GET": {
        String k = parts[1];
        if (expiry.containsKey(k) && System.currentTimeMillis() > expiry.get(k)) {
          store.remove(k); expiry.remove(k); return "$-1\r\n";
        }
        String v = store.get(k);
        return v == null ? "$-1\r\n" : "$" + v.length() + "\r\n" + v + "\r\n";
      }

      case "INCR": {
        String k = parts[1];
        String v = store.getOrDefault(k, "0");
        try {
          long num = Long.parseLong(v) + 1;
          store.put(k, String.valueOf(num));
          return ":" + num + "\r\n";
        } catch (NumberFormatException e) {
          return "-ERR value is not an integer or out of range\r\n";
        }
      }

      case "TYPE": {
        String k = parts[1];
        if (store.containsKey(k))         return "+string\r\n";
        if (listStore.containsKey(k))     return "+list\r\n";
        if (streamStore.containsKey(k))   return "+stream\r\n";
        return "+none\r\n";
      }

      case "MULTI":  return "+OK\r\n";
      case "EXEC":   return "-ERR EXEC without MULTI\r\n";
      case "DISCARD": return "-ERR DISCARD without MULTI\r\n";

      case "XADD": {
        String sKey = parts[1];
        String id   = parts[2];
        if (id.equals("*")) id = System.currentTimeMillis() + "-*";
        String[] idParts = id.split("-");
        long newMs  = Long.parseLong(idParts[0]);
        long newSeq;
        List<StreamEntry> stream = streamStore.computeIfAbsent(sKey, k -> new ArrayList<>());
        if (idParts[1].equals("*")) {
          if (!stream.isEmpty()) {
            String[] lp = stream.get(stream.size()-1).id.split("-");
            long lMs = Long.parseLong(lp[0]), lSeq = Long.parseLong(lp[1]);
            newSeq = (newMs == lMs) ? lSeq + 1 : (newMs == 0 ? 1 : 0);
          } else { newSeq = (newMs == 0) ? 1 : 0; }
          id = newMs + "-" + newSeq;
        } else { newSeq = Long.parseLong(idParts[1]); }
        if (newMs == 0 && newSeq == 0)
          return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        if (!stream.isEmpty()) {
          String[] lp = stream.get(stream.size()-1).id.split("-");
          long lMs = Long.parseLong(lp[0]), lSeq = Long.parseLong(lp[1]);
          if (newMs < lMs || (newMs == lMs && newSeq <= lSeq))
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        }
        StreamEntry entry = new StreamEntry(id);
        for (int i = 3; i + 1 < parts.length; i += 2) entry.fields.put(parts[i], parts[i+1]);
        stream.add(entry);
        notifyStreamWaiter(sKey);
        return "$" + id.length() + "\r\n" + id + "\r\n";
      }

      case "XRANGE": {
        String sKey = parts[1], start = parts[2], end = parts[3];
        long startMs, startSeq;
        if (start.equals("-")) { startMs = 0; startSeq = 0; }
        else { startMs = Long.parseLong(start.contains("-") ? start.split("-")[0] : start);
               startSeq = start.contains("-") ? Long.parseLong(start.split("-")[1]) : 0; }
        long endMs, endSeq;
        if (end.equals("+")) { endMs = Long.MAX_VALUE; endSeq = Long.MAX_VALUE; }
        else { endMs = Long.parseLong(end.contains("-") ? end.split("-")[0] : end);
               endSeq = end.contains("-") ? Long.parseLong(end.split("-")[1]) : Long.MAX_VALUE; }
        List<StreamEntry> stream = streamStore.get(sKey);
        List<StreamEntry> results = new ArrayList<>();
        if (stream != null) for (StreamEntry e : stream) {
          String[] ep = e.id.split("-");
          long eMs = Long.parseLong(ep[0]), eSeq = Long.parseLong(ep[1]);
          if ((eMs > startMs || (eMs == startMs && eSeq >= startSeq)) &&
              (eMs < endMs   || (eMs == endMs   && eSeq <= endSeq))) results.add(e);
        }
        return encodeEntries(results);
      }

      case "XREAD": {
        boolean blocking = parts[1].toUpperCase().equals("BLOCK");
        long blockMs = 0;
        int streamsIdx = blocking ? 3 : 1;
        if (blocking) blockMs = Long.parseLong(parts[2]);
        int numStreams = (parts.length - streamsIdx - 1) / 2;
        String[] xKeys = new String[numStreams];
        String[] xIds  = new String[numStreams];
        for (int i = 0; i < numStreams; i++) {
          xKeys[i] = parts[streamsIdx + 1 + i];
          xIds[i]  = parts[streamsIdx + 1 + numStreams + i];
          if (xIds[i].equals("$")) {
            List<StreamEntry> s = streamStore.get(xKeys[i]);
            xIds[i] = (s != null && !s.isEmpty()) ? s.get(s.size()-1).id : "0-0";
          }
        }
        if (blocking) {
          long deadline = blockMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + blockMs;
          Object lock = new Object();
          for (String k : xKeys) streamWaiters.computeIfAbsent(k, x -> new LinkedList<>()).add(lock);
          synchronized (lock) {
            while (true) {
              boolean hasResults = false;
              for (int i = 0; i < numStreams; i++) {
                List<StreamEntry> s = streamStore.get(xKeys[i]);
                if (s != null) {
                  String[] idP = xIds[i].split("-");
                  long aMs = Long.parseLong(idP[0]), aSeq = idP.length > 1 ? Long.parseLong(idP[1]) : 0;
                  for (StreamEntry e : s) {
                    String[] ep = e.id.split("-");
                    if (Long.parseLong(ep[0]) > aMs || (Long.parseLong(ep[0]) == aMs && Long.parseLong(ep[1]) > aSeq))
                    { hasResults = true; break; }
                  }
                }
                if (hasResults) break;
              }
              if (hasResults) break;
              long remaining = deadline - System.currentTimeMillis();
              if (remaining <= 0) {
                for (String k : xKeys) { LinkedList<Object> q = streamWaiters.get(k); if (q != null) q.remove(lock); }
                out.write("*-1\r\n".getBytes()); out.flush();
                return "";
              }
              lock.wait(remaining);
            }
          }
        }
        StringBuilder sb = new StringBuilder("*" + numStreams + "\r\n");
        for (int i = 0; i < numStreams; i++) {
          String xKey = xKeys[i], xId = xIds[i];
          String[] idP = xId.split("-");
          long afterMs = Long.parseLong(idP[0]), afterSeq = idP.length > 1 ? Long.parseLong(idP[1]) : 0;
          List<StreamEntry> stream = streamStore.get(xKey);
          List<StreamEntry> results = new ArrayList<>();
          if (stream != null) for (StreamEntry e : stream) {
            String[] ep = e.id.split("-");
            long eMs = Long.parseLong(ep[0]), eSeq = Long.parseLong(ep[1]);
            if (eMs > afterMs || (eMs == afterMs && eSeq > afterSeq)) results.add(e);
          }
          sb.append("*2\r\n$").append(xKey.length()).append("\r\n").append(xKey).append("\r\n");
          sb.append(encodeEntries(results));
        }
        return sb.toString();
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
            if (remaining <= 0) { LinkedList<Object> q = waiters.get(bKey); if (q != null) q.remove(lock); break; }
            lock.wait(remaining);
          }
        }
        return popped == null ? "*-1\r\n"
          : "*2\r\n$" + bKey.length() + "\r\n" + bKey + "\r\n$" + popped.length() + "\r\n" + popped + "\r\n";
      }

      case "LLEN": {
        List<String> l = listStore.get(parts[1]);
        return ":" + (l == null ? 0 : l.size()) + "\r\n";
      }

      case "LPOP": {
        List<String> pl = listStore.get(parts[1]);
        if (pl == null || pl.isEmpty()) return "$-1\r\n";
        synchronized (pl) {
          if (parts.length == 2) {
            String p = pl.remove(0);
            return "$" + p.length() + "\r\n" + p + "\r\n";
          } else {
            int count = Math.min(Integer.parseInt(parts[2]), pl.size());
            StringBuilder sb = new StringBuilder("*" + count + "\r\n");
            for (int i = 0; i < count; i++) { String el = pl.remove(0); sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n"); }
            return sb.toString();
          }
        }
      }

      case "LPUSH": {
        String k = parts[1]; listStore.putIfAbsent(k, new ArrayList<>()); List<String> l = listStore.get(k);
        synchronized (l) { for (int i = 2; i < parts.length; i++) l.add(0, parts[i]); }
        notifyWaiter(k);
        return ":" + listStore.get(k).size() + "\r\n";
      }

      case "RPUSH": {
        String k = parts[1]; listStore.putIfAbsent(k, new ArrayList<>()); List<String> l = listStore.get(k);
        synchronized (l) { for (int i = 2; i < parts.length; i++) l.add(parts[i]); }
        notifyWaiter(k);
        return ":" + listStore.get(k).size() + "\r\n";
      }

      case "LRANGE": {
        String k = parts[1]; int start = Integer.parseInt(parts[2]), stop = Integer.parseInt(parts[3]);
        List<String> l = listStore.get(k);
        if (l == null) return "*0\r\n";
        synchronized (l) {
          int size = l.size();
          if (start < 0) start = Math.max(0, size + start);
          if (stop  < 0) stop  = size + stop;
          if (start >= size || start > stop) return "*0\r\n";
          stop = Math.min(stop, size - 1);
          List<String> slice = l.subList(start, stop + 1);
          StringBuilder sb = new StringBuilder("*" + slice.size() + "\r\n");
          for (String el : slice) sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n");
          return sb.toString();
        }
      }

      default: return "-ERR unknown command\r\n";
    }
  }

  private static String encodeEntries(List<StreamEntry> entries) {
    StringBuilder sb = new StringBuilder("*" + entries.size() + "\r\n");
    for (StreamEntry e : entries) {
      sb.append("*2\r\n$").append(e.id.length()).append("\r\n").append(e.id).append("\r\n");
      sb.append("*").append(e.fields.size() * 2).append("\r\n");
      for (Map.Entry<String, String> f : e.fields.entrySet())
        sb.append("$").append(f.getKey().length()).append("\r\n").append(f.getKey()).append("\r\n")
          .append("$").append(f.getValue().length()).append("\r\n").append(f.getValue()).append("\r\n");
    }
    return sb.toString();
  }

  private static void notifyWaiter(String key) {
    LinkedList<Object> q = waiters.get(key);
    if (q != null) synchronized (q) { Object f = q.peek(); if (f != null) synchronized (f) { f.notify(); } }
  }

  private static void notifyStreamWaiter(String key) {
    LinkedList<Object> q = streamWaiters.get(key);
    if (q != null) synchronized (q) { for (Object w : q) synchronized (w) { w.notify(); } }
  }
}