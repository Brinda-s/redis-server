import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
  static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, List<String>> listStore = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, LinkedList<Object>> waiters = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, List<StreamEntry>> streamStore = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, LinkedList<Object>> streamWaiters = new ConcurrentHashMap<>();
  static String role = "master";
  static String dir = "";
  static String dbfilename = "";

  static class ReplicaState {
    OutputStream out;
    InputStream in;
    volatile long ackOffset = 0;
    ReplicaState(OutputStream out, InputStream in) { this.out = out; this.in = in; }
  }
  static CopyOnWriteArrayList<ReplicaState> replicas = new CopyOnWriteArrayList<>();
  static AtomicLong masterOffset = new AtomicLong(0);

  static class StreamEntry {
    String id;
    LinkedHashMap<String, String> fields = new LinkedHashMap<>();
    StreamEntry(String id) { this.id = id; }
  }

  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

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
      if (args[i].equals("--port"))       port       = Integer.parseInt(args[i + 1]);
      if (args[i].equals("--replicaof"))  role       = "slave";
      if (args[i].equals("--dir"))        dir        = args[i + 1];
      if (args[i].equals("--dbfilename")) dbfilename = args[i + 1];
    }

    if (!dir.isEmpty() && !dbfilename.isEmpty()) {
      loadRdb(dir + "/" + dbfilename);
    }

    try {
      ServerSocket serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      // If replica mode, do handshake with master in background thread
      if (masterHost != null) {
        final String mHost = masterHost;
        final int mPort = masterPort;
        final int fPort = port;
        new Thread(() -> {
          try {
            Socket masterSocket = new Socket(mHost, mPort);
            masterSocket.setTcpNoDelay(true);
            InputStream masterRawIn = masterSocket.getInputStream();
            OutputStream masterOut = masterSocket.getOutputStream();

            masterOut.write("*1\r\n$4\r\nPING\r\n".getBytes()); masterOut.flush();
            readLineRaw(masterRawIn); // +PONG

            String portStr = String.valueOf(fPort);
            masterOut.write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"
                + portStr.length() + "\r\n" + portStr + "\r\n").getBytes());
            masterOut.flush();
            readLineRaw(masterRawIn); // +OK

            masterOut.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
            masterOut.flush();
            readLineRaw(masterRawIn); // +OK

            masterOut.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
            masterOut.flush();
            readLineRaw(masterRawIn); // +FULLRESYNC

            // Skip RDB file
            String rdbHeader = readLineRaw(masterRawIn);
            int rdbLen = Integer.parseInt(rdbHeader.substring(1).trim());
            byte[] rdbBuf = new byte[rdbLen];
            int totalRead = 0;
            while (totalRead < rdbLen) {
              int n = masterRawIn.read(rdbBuf, totalRead, rdbLen - totalRead);
              if (n == -1) break;
              totalRead += n;
            }

            // Propagation loop — process commands from master silently
            long replicaOffset = 0;
            String line;
            while ((line = readLineRaw(masterRawIn)) != null) {
              if (!line.startsWith("*")) continue;
              int numArgs = Integer.parseInt(line.substring(1));
              String[] parts = new String[numArgs];
              for (int i = 0; i < numArgs; i++) {
                readLineRaw(masterRawIn); // skip $len
                parts[i] = readLineRaw(masterRawIn);
              }
              if (parts[0] == null) continue;

              // Calculate byte size of this command
              StringBuilder respCmd = new StringBuilder("*" + numArgs + "\r\n");
              for (String p : parts) respCmd.append("$").append(p.length()).append("\r\n").append(p).append("\r\n");
              int cmdBytes = respCmd.toString().getBytes().length;

              if (parts[0].toUpperCase().equals("REPLCONF") && parts.length > 1
                  && parts[1].toUpperCase().equals("GETACK")) {
                // Respond with offset BEFORE counting this GETACK
                String ackOffset = String.valueOf(replicaOffset);
                masterOut.write(("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$"
                    + ackOffset.length() + "\r\n" + ackOffset + "\r\n").getBytes());
                masterOut.flush();
              } else {
                try { execCommand(parts[0].toUpperCase(), parts, null); } catch (Exception ignored) {}
              }
              replicaOffset += cmdBytes;
            }
          } catch (Exception e) {
            System.err.println("Replica handshake error: " + e.getMessage());
          }
        }).start();
      }

      // Accept client connections
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static void handleClient(Socket clientSocket) {
    boolean isReplica = false;
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      OutputStream out = clientSocket.getOutputStream();
      boolean inMulti = false;
      List<String[]> txQueue = new ArrayList<>();
      Set<String> subscribedChannels = new HashSet<>();

      String line;
      while ((line = in.readLine()) != null) {
        if (!line.startsWith("*")) continue;
        int numArgs = Integer.parseInt(line.substring(1));
        String[] parts = new String[numArgs];
        for (int i = 0; i < numArgs; i++) { in.readLine(); parts[i] = in.readLine(); }
        String command = parts[0].toUpperCase();

        // Queue commands inside MULTI block
        if (inMulti && !command.equals("EXEC") && !command.equals("DISCARD")) {
          txQueue.add(parts);
          out.write("+QUEUED\r\n".getBytes());
          out.flush();
          continue;
        }

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

        } else if (command.equals("SUBSCRIBE")) {
          // Per-client subscription tracking with deduplication
          StringBuilder sb = new StringBuilder();
          for (int i = 1; i < parts.length; i++) {
            String ch = parts[i];
            subscribedChannels.add(ch); // HashSet deduplicates
            sb.append("*3\r\n$9\r\nsubscribe\r\n$").append(ch.length()).append("\r\n").append(ch)
              .append("\r\n:").append(subscribedChannels.size()).append("\r\n");
          }
          out.write(sb.toString().getBytes());

        } else {
          String resp = execCommand(command, parts, out);
          if (!resp.isEmpty()) out.write(resp.getBytes());
          // After PSYNC, hand off this connection to the replica ACK reader
          if (command.equals("PSYNC")) {
            ReplicaState rs = new ReplicaState(out, clientSocket.getInputStream());
            replicas.add(rs);
            out.flush();
            startAckReader(rs, in);
            isReplica = true;
            return;
          }
        }
        out.flush();
      }
    } catch (IOException | InterruptedException e) {
      System.out.println("Exception: " + e.getMessage());
    } finally {
      // Don't close replica sockets — they stay open for propagation
      if (!isReplica) {
        try { clientSocket.close(); } catch (IOException ignored) {}
      }
    }
  }

  private static void startAckReader(ReplicaState rs, BufferedReader reader) {
    new Thread(() -> {
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          if (!line.startsWith("*")) continue;
          int n = Integer.parseInt(line.substring(1));
          String[] parts = new String[n];
          for (int i = 0; i < n; i++) { reader.readLine(); parts[i] = reader.readLine(); }
          if (n == 3 && parts[0].toUpperCase().equals("REPLCONF")
              && parts[1].toUpperCase().equals("ACK")) {
            rs.ackOffset = Long.parseLong(parts[2]);
          }
        }
      } catch (IOException ignored) {}
    }).start();
  }

  private static String execCommand(String command, String[] parts, OutputStream out)
      throws InterruptedException, IOException {
    switch (command) {

      case "PING": return "+PONG\r\n";

      case "ECHO":
        return "$" + parts[1].length() + "\r\n" + parts[1] + "\r\n";

      case "SET":
        store.put(parts[1], parts[2]);
        if (parts.length >= 5 && parts[3].toUpperCase().equals("PX"))
          expiry.put(parts[1], System.currentTimeMillis() + Long.parseLong(parts[4]));
        else
          expiry.remove(parts[1]);
        propagate(parts);
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
        if (store.containsKey(k))       return "+string\r\n";
        if (listStore.containsKey(k))   return "+list\r\n";
        if (streamStore.containsKey(k)) return "+stream\r\n";
        return "+none\r\n";
      }

      case "INFO": {
        String info = "role:" + role + "\r\n"
                    + "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
                    + "master_repl_offset:0";
        return "$" + info.length() + "\r\n" + info + "\r\n";
      }

      case "PSYNC": {
        if (out == null) return "";
        byte[] rdb = java.util.Base64.getDecoder().decode(
          "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
        out.write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".getBytes());
        out.write(("$" + rdb.length + "\r\n").getBytes());
        out.write(rdb);
        out.flush();
        return "";
      }

      case "REPLCONF": return "+OK\r\n";

      case "WAIT": {
        int needed  = Integer.parseInt(parts[1]);
        long timeout = Long.parseLong(parts[2]);

        // No writes yet — all replicas are already in sync
        if (masterOffset.get() == 0) return ":" + replicas.size() + "\r\n";

        // Ask all replicas for their current offset
        byte[] getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".getBytes();
        for (ReplicaState rs : replicas) {
          try { rs.out.write(getack); rs.out.flush(); } catch (IOException ignored) {}
        }

        // Poll until enough ack or timeout
        long deadline = System.currentTimeMillis() + timeout;
        long target   = masterOffset.get();
        while (System.currentTimeMillis() < deadline) {
          int acked = 0;
          for (ReplicaState rs : replicas) if (rs.ackOffset >= target) acked++;
          if (acked >= needed) return ":" + acked + "\r\n";
          Thread.sleep(10);
        }
        int acked = 0;
        for (ReplicaState rs : replicas) if (rs.ackOffset >= target) acked++;
        return ":" + acked + "\r\n";
      }

      case "CONFIG": {
        if (parts.length >= 3 && parts[1].toUpperCase().equals("GET")) {
          String param = parts[2].toLowerCase();
          String value = param.equals("dir") ? dir : param.equals("dbfilename") ? dbfilename : "";
          return "*2\r\n$" + param.length() + "\r\n" + param + "\r\n$"
              + value.length() + "\r\n" + value + "\r\n";
        }
        return "-ERR unknown config command\r\n";
      }

      case "KEYS": {
        List<String> keys = new ArrayList<>(store.keySet());
        StringBuilder sb = new StringBuilder("*" + keys.size() + "\r\n");
        for (String k : keys) sb.append("$").append(k.length()).append("\r\n").append(k).append("\r\n");
        return sb.toString();
      }

      case "MULTI":   return "+OK\r\n";
      case "EXEC":    return "-ERR EXEC without MULTI\r\n";
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
            String[] lp = stream.get(stream.size() - 1).id.split("-");
            long lMs = Long.parseLong(lp[0]), lSeq = Long.parseLong(lp[1]);
            newSeq = (newMs == lMs) ? lSeq + 1 : (newMs == 0 ? 1 : 0);
          } else {
            newSeq = (newMs == 0) ? 1 : 0;
          }
          id = newMs + "-" + newSeq;
        } else {
          newSeq = Long.parseLong(idParts[1]);
        }
        if (newMs == 0 && newSeq == 0)
          return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        if (!stream.isEmpty()) {
          String[] lp = stream.get(stream.size() - 1).id.split("-");
          long lMs = Long.parseLong(lp[0]), lSeq = Long.parseLong(lp[1]);
          if (newMs < lMs || (newMs == lMs && newSeq <= lSeq))
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        }
        StreamEntry entry = new StreamEntry(id);
        for (int i = 3; i + 1 < parts.length; i += 2) entry.fields.put(parts[i], parts[i + 1]);
        stream.add(entry);
        notifyStreamWaiter(sKey);
        return "$" + id.length() + "\r\n" + id + "\r\n";
      }

      case "XRANGE": {
        String sKey = parts[1], start = parts[2], end = parts[3];
        long startMs, startSeq;
        if (start.equals("-")) { startMs = 0; startSeq = 0; }
        else {
          startMs  = Long.parseLong(start.contains("-") ? start.split("-")[0] : start);
          startSeq = start.contains("-") ? Long.parseLong(start.split("-")[1]) : 0;
        }
        long endMs, endSeq;
        if (end.equals("+")) { endMs = Long.MAX_VALUE; endSeq = Long.MAX_VALUE; }
        else {
          endMs  = Long.parseLong(end.contains("-") ? end.split("-")[0] : end);
          endSeq = end.contains("-") ? Long.parseLong(end.split("-")[1]) : Long.MAX_VALUE;
        }
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
            xIds[i] = (s != null && !s.isEmpty()) ? s.get(s.size() - 1).id : "0-0";
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
                for (String k : xKeys) {
                  LinkedList<Object> q = streamWaiters.get(k);
                  if (q != null) q.remove(lock);
                }
                if (out != null) { out.write("*-1\r\n".getBytes()); out.flush(); }
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
          long afterMs  = Long.parseLong(idP[0]);
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
            if (remaining <= 0) {
              LinkedList<Object> q = waiters.get(bKey);
              if (q != null) q.remove(lock);
              break;
            }
            lock.wait(remaining);
          }
        }
        if (popped == null) return "*-1\r\n";
        return "*2\r\n$" + bKey.length() + "\r\n" + bKey + "\r\n$" + popped.length() + "\r\n" + popped + "\r\n";
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
            for (int i = 0; i < count; i++) {
              String el = pl.remove(0);
              sb.append("$").append(el.length()).append("\r\n").append(el).append("\r\n");
            }
            return sb.toString();
          }
        }
      }

      case "LPUSH": {
        String k = parts[1];
        listStore.putIfAbsent(k, new ArrayList<>());
        List<String> l = listStore.get(k);
        int size;
        synchronized (l) {
          for (int i = 2; i < parts.length; i++) l.add(0, parts[i]);
          size = l.size();
        }
        notifyWaiter(k);
        return ":" + size + "\r\n";
      }

      case "RPUSH": {
        String k = parts[1];
        listStore.putIfAbsent(k, new ArrayList<>());
        List<String> l = listStore.get(k);
        int size;
        synchronized (l) {
          for (int i = 2; i < parts.length; i++) l.add(parts[i]);
          size = l.size();
        }
        notifyWaiter(k);
        return ":" + size + "\r\n";
      }

      case "LRANGE": {
        String k = parts[1];
        int start = Integer.parseInt(parts[2]);
        int stop  = Integer.parseInt(parts[3]);
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

  // ── Helpers ──────────────────────────────────────────────────────────────

  private static String encodeEntries(List<StreamEntry> entries) {
    StringBuilder sb = new StringBuilder("*" + entries.size() + "\r\n");
    for (StreamEntry e : entries) {
      sb.append("*2\r\n$").append(e.id.length()).append("\r\n").append(e.id).append("\r\n");
      sb.append("*").append(e.fields.size() * 2).append("\r\n");
      for (Map.Entry<String, String> f : e.fields.entrySet()) {
        sb.append("$").append(f.getKey().length()).append("\r\n").append(f.getKey()).append("\r\n");
        sb.append("$").append(f.getValue().length()).append("\r\n").append(f.getValue()).append("\r\n");
      }
    }
    return sb.toString();
  }

  private static void propagate(String[] parts) {
    StringBuilder sb = new StringBuilder("*" + parts.length + "\r\n");
    for (String p : parts) sb.append("$").append(p.length()).append("\r\n").append(p).append("\r\n");
    byte[] msg = sb.toString().getBytes();
    masterOffset.addAndGet(msg.length);
    for (ReplicaState rs : replicas) {
      try { rs.out.write(msg); rs.out.flush(); } catch (IOException e) { replicas.remove(rs); }
    }
  }

  private static void notifyWaiter(String key) {
    LinkedList<Object> q = waiters.get(key);
    if (q != null) synchronized (q) {
      Object f = q.peek();
      if (f != null) synchronized (f) { f.notify(); }
    }
  }

  private static void notifyStreamWaiter(String key) {
    LinkedList<Object> q = streamWaiters.get(key);
    if (q != null) synchronized (q) {
      for (Object w : q) synchronized (w) { w.notify(); }
    }
  }

  private static String readLineRaw(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    int c;
    while ((c = in.read()) != -1) {
      if (c == '\n') break;
      if (c != '\r') sb.append((char) c);
    }
    if (c == -1 && sb.length() == 0) return null;
    return sb.toString();
  }

  // ── RDB Loader ───────────────────────────────────────────────────────────

  private static void loadRdb(String path) {
    try (DataInputStream dis = new DataInputStream(new FileInputStream(path))) {
      byte[] magic = new byte[9];
      dis.readFully(magic); // skip "REDIS0011"

      while (true) {
        int op = dis.read();
        if (op == -1 || op == 0xFF) break; // EOF

        if (op == 0xFA) { readRdbString(dis); readRdbString(dis); continue; } // aux field
        if (op == 0xFE) { readRdbLength(dis); continue; }                     // db selector
        if (op == 0xFB) { readRdbLength(dis); readRdbLength(dis); continue; } // resize db

        long expireMs = -1;
        int valueType = op;

        if (op == 0xFC) { expireMs = readLongLE(dis); valueType = dis.read(); }       // ms expiry
        else if (op == 0xFD) { expireMs = readIntLE(dis) * 1000L; valueType = dis.read(); } // sec expiry

        if (valueType == 0) {
          String key = readRdbString(dis);
          String val = readRdbString(dis);
          store.put(key, val);
          if (expireMs > 0) expiry.put(key, expireMs);
        } else {
          break; // unsupported type
        }
      }
    } catch (FileNotFoundException ignored) {
      // No RDB file — start with empty DB
    } catch (Exception e) {
      System.out.println("RDB load error: " + e.getMessage());
    }
  }

  private static int readRdbLength(DataInputStream dis) throws IOException {
    int first = dis.read();
    int enc = (first & 0xC0) >> 6;
    if (enc == 0) return first & 0x3F;
    if (enc == 1) return ((first & 0x3F) << 8) | dis.read();
    if (enc == 2) return (dis.read() << 24) | (dis.read() << 16) | (dis.read() << 8) | dis.read();
    return -(first & 0x3F); // special encoding
  }

  private static String readRdbString(DataInputStream dis) throws IOException {
    int first = dis.read();
    int enc = (first & 0xC0) >> 6;
    if (enc == 3) {
      int subtype = first & 0x3F;
      if (subtype == 0) return String.valueOf(dis.read());
      if (subtype == 1) return String.valueOf(dis.read() | (dis.read() << 8));
      if (subtype == 2) return String.valueOf(readIntLE(dis));
      return "";
    }
    int len;
    if (enc == 0)      len = first & 0x3F;
    else if (enc == 1) len = ((first & 0x3F) << 8) | dis.read();
    else               len = (dis.read() << 24) | (dis.read() << 16) | (dis.read() << 8) | dis.read();
    byte[] bytes = new byte[len];
    dis.readFully(bytes);
    return new String(bytes);
  }

  private static long readLongLE(DataInputStream dis) throws IOException {
    long v = 0;
    for (int i = 0; i < 8; i++) v |= ((long)(dis.read() & 0xFF)) << (8 * i);
    return v;
  }

  private static int readIntLE(DataInputStream dis) throws IOException {
    int v = 0;
    for (int i = 0; i < 4; i++) v |= (dis.read() & 0xFF) << (8 * i);
    return v;
  }
}