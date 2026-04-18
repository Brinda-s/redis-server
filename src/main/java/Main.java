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
  static ConcurrentHashMap<String, LinkedList<Object>> streamWaiters = new ConcurrentHashMap<>();

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
    } catch (IOException e) { System.out.println("IOException: " + e.getMessage()); }
  }

  private static void handleClient(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      OutputStream out = clientSocket.getOutputStream();
      String line;
      while ((line = in.readLine()) != null) {
        if (!line.startsWith("*")) continue;
        int numArgs = Integer.parseInt(line.substring(1));
        String[] parts = new String[numArgs];
        for (int i = 0; i < numArgs; i++) { in.readLine(); parts[i] = in.readLine(); }
        String command = parts[0].toUpperCase();

        switch (command) {
          case "EXEC":
            out.write("-ERR EXEC without MULTI\r\n".getBytes()); break;

          case "MULTI":
            out.write("+OK\r\n".getBytes()); break;

          case "INCR": {
            String k = parts[1];
            String v = store.get(k);
            if (v == null) v = "0"; // key doesn't exist → treat as 0
            try {
              long num = Long.parseLong(v) + 1;
              store.put(k, String.valueOf(num));
              out.write((":" + num + "\r\n").getBytes());
            } catch (NumberFormatException e) {
              out.write("-ERR value is not an integer or out of range\r\n".getBytes());
            }
            break;
          }

          case "PING": out.write("+PONG\r\n".getBytes()); break;

          case "ECHO":
            out.write(("$" + parts[1].length() + "\r\n" + parts[1] + "\r\n").getBytes()); break;

          case "SET":
            store.put(parts[1], parts[2]);
            if (parts.length >= 5 && parts[3].toUpperCase().equals("PX"))
              expiry.put(parts[1], System.currentTimeMillis() + Long.parseLong(parts[4]));
            else expiry.remove(parts[1]);
            out.write("+OK\r\n".getBytes()); break;

          case "GET": {
            String k = parts[1];
            if (expiry.containsKey(k) && System.currentTimeMillis() > expiry.get(k)) {
              store.remove(k); expiry.remove(k); out.write("$-1\r\n".getBytes());
            } else {
              String v = store.get(k);
              out.write(v == null ? "$-1\r\n".getBytes() : ("$" + v.length() + "\r\n" + v + "\r\n").getBytes());
            }
            break;
          }

          case "TYPE": {
            String k = parts[1];
            if (store.containsKey(k))        out.write("+string\r\n".getBytes());
            else if (listStore.containsKey(k))   out.write("+list\r\n".getBytes());
            else if (streamStore.containsKey(k)) out.write("+stream\r\n".getBytes());
            else                              out.write("+none\r\n".getBytes());
            break;
          }

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
            if (newMs == 0 && newSeq == 0) {
              out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes()); break;
            }
            if (!stream.isEmpty()) {
              String[] lp = stream.get(stream.size()-1).id.split("-");
              long lMs = Long.parseLong(lp[0]), lSeq = Long.parseLong(lp[1]);
              if (newMs < lMs || (newMs == lMs && newSeq <= lSeq)) {
                out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes()); break;
              }
            }
            StreamEntry entry = new StreamEntry(id);
            for (int i = 3; i + 1 < parts.length; i += 2) entry.fields.put(parts[i], parts[i+1]);
            stream.add(entry);
            out.write(("$" + id.length() + "\r\n" + id + "\r\n").getBytes());
            notifyStreamWaiter(sKey);
            break;
          }

          case "XRANGE": {
            String sKey  = parts[1];
            String start = parts[2], end = parts[3];
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
            if (stream != null) {
              for (StreamEntry e : stream) {
                String[] ep = e.id.split("-");
                long eMs = Long.parseLong(ep[0]), eSeq = Long.parseLong(ep[1]);
                if ((eMs > startMs || (eMs == startMs && eSeq >= startSeq)) &&
                    (eMs < endMs   || (eMs == endMs   && eSeq <= endSeq)))
                  results.add(e);
              }
            }
            out.write(encodeEntries(results).getBytes());
            break;
          }

          case "XREAD": {
            // Check for BLOCK option: XREAD BLOCK <ms> STREAMS key... id...
            boolean blocking = parts[1].toUpperCase().equals("BLOCK");
            long blockMs = 0;
            int streamsIdx; // index of "STREAMS" keyword
            if (blocking) { blockMs = Long.parseLong(parts[2]); streamsIdx = 3; }
            else { streamsIdx = 1; }
            // parts[streamsIdx] = "STREAMS"
            int numStreams = (parts.length - streamsIdx - 1) / 2;
            String[] xKeys = new String[numStreams];
            String[] xIds  = new String[numStreams];
            for (int i = 0; i < numStreams; i++) {
              xKeys[i] = parts[streamsIdx + 1 + i];
              xIds[i]  = parts[streamsIdx + 1 + numStreams + i];
              // $ means "only entries added after this command" → resolve to current last ID
              if (xIds[i].equals("$")) {
                List<StreamEntry> s = streamStore.get(xKeys[i]);
                xIds[i] = (s != null && !s.isEmpty()) ? s.get(s.size()-1).id : "0-0";
              }
            }

            // Helper: collect results for all streams
            // Returns null-per-stream if nothing found
            if (blocking) {
              long deadline = blockMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + blockMs;
              // Register waiters for all requested keys
              Object lock = new Object();
              for (String k : xKeys)
                streamWaiters.computeIfAbsent(k, x -> new LinkedList<>()).add(lock);

              synchronized (lock) {
                while (true) {
                  // Check if any results are available
                  boolean hasResults = false;
                  for (int i = 0; i < numStreams; i++) {
                    List<StreamEntry> s = streamStore.get(xKeys[i]);
                    if (s != null && !s.isEmpty()) {
                      String[] idP = xIds[i].split("-");
                      long aMs = Long.parseLong(idP[0]);
                      long aSeq = idP.length > 1 ? Long.parseLong(idP[1]) : 0;
                      for (StreamEntry e : s) {
                        String[] ep = e.id.split("-");
                        if (Long.parseLong(ep[0]) > aMs ||
                            (Long.parseLong(ep[0]) == aMs && Long.parseLong(ep[1]) > aSeq)) {
                          hasResults = true; break;
                        }
                      }
                    }
                    if (hasResults) break;
                  }
                  if (hasResults) break;
                  long remaining = deadline - System.currentTimeMillis();
                  if (remaining <= 0) {
                    for (String k : xKeys) { LinkedList<Object> q = streamWaiters.get(k); if (q != null) q.remove(lock); }
                    out.write("*-1\r\n".getBytes());
                    out.flush();
                    break;
                  }
                  lock.wait(remaining);
                }
              }
            }

            // Build response
            StringBuilder sb = new StringBuilder("*" + numStreams + "\r\n");
            for (int i = 0; i < numStreams; i++) {
              String xKey = xKeys[i], xId = xIds[i];
              String[] idP = xId.split("-");
              long afterMs = Long.parseLong(idP[0]);
              long afterSeq = idP.length > 1 ? Long.parseLong(idP[1]) : 0;
              List<StreamEntry> stream = streamStore.get(xKey);
              List<StreamEntry> results = new ArrayList<>();
              if (stream != null) {
                for (StreamEntry e : stream) {
                  String[] ep = e.id.split("-");
                  long eMs = Long.parseLong(ep[0]), eSeq = Long.parseLong(ep[1]);
                  if (eMs > afterMs || (eMs == afterMs && eSeq > afterSeq)) results.add(e);
                }
              }
              sb.append("*2\r\n");
              sb.append("$").append(xKey.length()).append("\r\n").append(xKey).append("\r\n");
              sb.append(encodeEntries(results));
            }
            // Only write if not already written null response
            if (!blocking || !sb.toString().contains("*-1")) out.write(sb.toString().getBytes());
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
                if (remaining <= 0) { LinkedList<Object> q = waiters.get(bKey); if (q != null) q.remove(lock); break; }
                lock.wait(remaining);
              }
            }
            if (popped == null) out.write("*-1\r\n".getBytes());
            else out.write(("*2\r\n$" + bKey.length() + "\r\n" + bKey + "\r\n$" + popped.length() + "\r\n" + popped + "\r\n").getBytes());
            break;
          }

          case "LLEN": {
            List<String> l = listStore.get(parts[1]);
            out.write((":" + (l == null ? 0 : l.size()) + "\r\n").getBytes()); break;
          }

          case "LPOP": {
            List<String> pl = listStore.get(parts[1]);
            if (pl == null || pl.isEmpty()) { out.write("$-1\r\n".getBytes()); }
            else synchronized (pl) {
              if (parts.length == 2) {
                String p = pl.remove(0);
                out.write(("$" + p.length() + "\r\n" + p + "\r\n").getBytes());
              } else {
                int count = Math.min(Integer.parseInt(parts[2]), pl.size());
                StringBuilder sb = new StringBuilder("*" + count + "\r\n");
                for (int i = 0; i < count; i++) { String el = pl.remove(0); sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n"); }
                out.write(sb.toString().getBytes());
              }
            }
            break;
          }

          case "LPUSH": {
            String k = parts[1]; listStore.putIfAbsent(k, new ArrayList<>()); List<String> l = listStore.get(k);
            synchronized (l) { for (int i = 2; i < parts.length; i++) l.add(0, parts[i]); out.write((":" + l.size() + "\r\n").getBytes()); }
            notifyWaiter(k); break;
          }

          case "RPUSH": {
            String k = parts[1]; listStore.putIfAbsent(k, new ArrayList<>()); List<String> l = listStore.get(k);
            synchronized (l) { for (int i = 2; i < parts.length; i++) l.add(parts[i]); out.write((":" + l.size() + "\r\n").getBytes()); }
            notifyWaiter(k); break;
          }

          case "LRANGE": {
            String k = parts[1]; int start = Integer.parseInt(parts[2]), stop = Integer.parseInt(parts[3]);
            List<String> l = listStore.get(k);
            if (l == null) { out.write("*0\r\n".getBytes()); break; }
            synchronized (l) {
              int size = l.size();
              if (start < 0) start = Math.max(0, size + start);
              if (stop  < 0) stop  = size + stop;
              if (start >= size || start > stop) { out.write("*0\r\n".getBytes()); break; }
              stop = Math.min(stop, size - 1);
              List<String> slice = l.subList(start, stop + 1);
              StringBuilder sb = new StringBuilder("*" + slice.size() + "\r\n");
              for (String el : slice) sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n");
              out.write(sb.toString().getBytes());
            }
            break;
          }
        }
        out.flush();
      }
    } catch (IOException | InterruptedException e) { System.out.println("Exception: " + e.getMessage()); }
    finally { try { clientSocket.close(); } catch (IOException e) { System.out.println("IOException: " + e.getMessage()); } }
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
    if (q != null) synchronized (q) { Object first = q.peek(); if (first != null) synchronized (first) { first.notify(); } }
  }

  private static void notifyStreamWaiter(String key) {
    LinkedList<Object> q = streamWaiters.get(key);
    if (q != null) synchronized (q) { for (Object w : q) synchronized (w) { w.notify(); } }
  }
}