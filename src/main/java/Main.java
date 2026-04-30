import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
  static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, List<String>> listStore = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, LinkedList<Object>> waiters = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, List<StreamEntry>> streamStore = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, LinkedList<Object>> streamWaiters = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, Set<OutputStream>> pubsubChannels = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, TreeMap<String, Double>> zsetStore = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, Set<AtomicBoolean>> keyDirtyFlags = new ConcurrentHashMap<>();
  static String role = "master";
  static String dir = System.getProperty("user.dir");
  static String dbfilename = "";
  static String appendonly = "no";
  static String appenddirname = "appendonlydir";
  static String appendfilename = "appendonly.aof";
  static String appendfsync = "everysec";
  static java.io.FileOutputStream aofStream = null;

  static class ReplicaState {
    OutputStream out;
    InputStream in;
    volatile long ackOffset = 0;
    ReplicaState(OutputStream out, InputStream in) { this.out = out; this.in = in; }
  }
  static CopyOnWriteArrayList<ReplicaState> replicas = new CopyOnWriteArrayList<>();
  static AtomicLong masterOffset = new AtomicLong(0);

  static boolean defaultUserNopass = true;
  static List<String> defaultUserPasswords = new ArrayList<>();

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
      if (args[i].equals("--appendonly"))     appendonly     = args[i + 1];
      if (args[i].equals("--appenddirname"))  appenddirname  = args[i + 1];
      if (args[i].equals("--appendfilename")) appendfilename = args[i + 1];
      if (args[i].equals("--appendfsync"))    appendfsync    = args[i + 1];
    }

    if (!dir.isEmpty() && !dbfilename.isEmpty()) {
      loadRdb(dir + "/" + dbfilename);
    }
  
    if (appendonly.equals("yes")) {
      new java.io.File(dir + "/" + appenddirname).mkdirs();
      java.io.File manifestFile = new java.io.File(dir + "/" + appenddirname + "/" + appendfilename + ".manifest");
      // Only create default files if manifest doesn't already exist
      if (!manifestFile.exists()) {
        try {
          new java.io.File(dir + "/" + appenddirname + "/" + appendfilename + ".1.incr.aof").createNewFile();
          java.nio.file.Files.writeString(
            manifestFile.toPath(),
            "file " + appendfilename + ".1.incr.aof seq 1 type i\n"
          );
        } catch (IOException ignored) {}
      }
      // Always read manifest to find active AOF file
      try {
        for (String mLine : java.nio.file.Files.readAllLines(manifestFile.toPath())) {
          if (mLine.contains("type i")) {
            String[] tokens = mLine.trim().split("\\s+");
            String aofFileName = tokens[1];
            replayAof(dir + "/" + appenddirname + "/" + aofFileName);
            aofStream = new java.io.FileOutputStream(dir + "/" + appenddirname + "/" + aofFileName, true);
            break;
          }
        }
      } catch (IOException ignored) {}
    }
  
      
    try {
      ServerSocket serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

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
            readLineRaw(masterRawIn);

            String portStr = String.valueOf(fPort);
            masterOut.write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"
                + portStr.length() + "\r\n" + portStr + "\r\n").getBytes());
            masterOut.flush();
            readLineRaw(masterRawIn);

            masterOut.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
            masterOut.flush();
            readLineRaw(masterRawIn);

            masterOut.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
            masterOut.flush();
            readLineRaw(masterRawIn);

            String rdbHeader = readLineRaw(masterRawIn);
            int rdbLen = Integer.parseInt(rdbHeader.substring(1).trim());
            byte[] rdbBuf = new byte[rdbLen];
            int totalRead = 0;
            while (totalRead < rdbLen) {
              int n = masterRawIn.read(rdbBuf, totalRead, rdbLen - totalRead);
              if (n == -1) break;
              totalRead += n;
            }

            long replicaOffset = 0;
            String line;
            while ((line = readLineRaw(masterRawIn)) != null) {
              if (!line.startsWith("*")) continue;
              int numArgs = Integer.parseInt(line.substring(1));
              String[] parts = new String[numArgs];
              for (int i = 0; i < numArgs; i++) {
                readLineRaw(masterRawIn);
                parts[i] = readLineRaw(masterRawIn);
              }
              if (parts[0] == null) continue;

              StringBuilder respCmd = new StringBuilder("*" + numArgs + "\r\n");
              for (String p : parts) respCmd.append("$").append(p.length()).append("\r\n").append(p).append("\r\n");
              int cmdBytes = respCmd.toString().getBytes().length;

              if (parts[0].toUpperCase().equals("REPLCONF") && parts.length > 1
                  && parts[1].toUpperCase().equals("GETACK")) {
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
    AtomicBoolean watchDirty = new AtomicBoolean(false);
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      OutputStream out = clientSocket.getOutputStream();
      boolean inMulti = false;
      List<String[]> txQueue = new ArrayList<>();
      Set<String> subscribedChannels = new HashSet<>();
      boolean inSubscribed = false;
      boolean authenticated = defaultUserNopass; 
      Set<String> watchedKeys = new HashSet<>();

      String line;
      while ((line = in.readLine()) != null) {
        // Handle inline commands (e.g. plain PING in subscribed mode)
        if (!line.startsWith("*")) {
          if (inSubscribed && line.trim().toUpperCase().equals("PING")) {
            out.write("*2\r\n$4\r\npong\r\n$0\r\n\r\n".getBytes());
            out.flush();
          }
          continue;
        }

        int numArgs = Integer.parseInt(line.substring(1));
        String[] parts = new String[numArgs];
        for (int i = 0; i < numArgs; i++) { in.readLine(); parts[i] = in.readLine(); }
        String command = (parts[0] != null) ? parts[0].toUpperCase() : "NULL";

        // Queue commands inside MULTI block
        if (inMulti && !command.equals("EXEC") && !command.equals("DISCARD") && !command.equals("WATCH")) {
          txQueue.add(parts);
          out.write("+QUEUED\r\n".getBytes());
          out.flush();
          continue;
        }

        if (command.equals("MULTI")) {
          inMulti = true;
          out.write("+OK\r\n".getBytes());

        }  else if (command.equals("EXEC")) {
          if (!inMulti) {
            out.write("-ERR EXEC without MULTI\r\n".getBytes());
          } else {
            inMulti = false;
            if (watchDirty.get()) {
              txQueue.clear();
              out.write("*-1\r\n".getBytes());
            } else {
              StringBuilder sb = new StringBuilder("*" + txQueue.size() + "\r\n");
              for (String[] cmd : txQueue) sb.append(execCommand(cmd[0].toUpperCase(), cmd, out));
              txQueue.clear();
              out.write(sb.toString().getBytes());
            }
            // clear watch state
            for (String k : watchedKeys) {
              Set<AtomicBoolean> flags = keyDirtyFlags.get(k);
              if (flags != null) flags.remove(watchDirty);
            }
            watchedKeys.clear();
            watchDirty.set(false);
          }

        } else if (command.equals("DISCARD")) {
          if (!inMulti) {
            out.write("-ERR DISCARD without MULTI\r\n".getBytes());
          } else {
            inMulti = false;
            txQueue.clear();
            // ADD THESE 4 LINES:
            for (String k : watchedKeys) {
              Set<AtomicBoolean> flags = keyDirtyFlags.get(k);
              if (flags != null) flags.remove(watchDirty);
            }
            watchedKeys.clear();
            watchDirty.set(false);
            out.write("+OK\r\n".getBytes());
          }
          } else if (command.equals("SUBSCRIBE") || command.equals("UNSUBSCRIBE")) {
          StringBuilder sb = new StringBuilder();
          for (int i = 1; i < parts.length; i++) {
            String ch = parts[i];
            if (command.equals("SUBSCRIBE")) {
              subscribedChannels.add(ch);
              pubsubChannels.computeIfAbsent(ch, k -> Collections.synchronizedSet(new HashSet<>())).add(out);
              sb.append("*3\r\n$9\r\nsubscribe\r\n$").append(ch.length()).append("\r\n").append(ch)
                .append("\r\n:").append(subscribedChannels.size()).append("\r\n");
            } else {
              subscribedChannels.remove(ch);
              Set<OutputStream> subs = pubsubChannels.get(ch);
              if (subs != null) subs.remove(out);
              sb.append("*3\r\n$11\r\nunsubscribe\r\n$").append(ch.length()).append("\r\n").append(ch)
                .append("\r\n:").append(subscribedChannels.size()).append("\r\n");
            }
          }
          inSubscribed = !subscribedChannels.isEmpty();
          out.write(sb.toString().getBytes());

        } else if (inSubscribed) {
          if (command.equals("PING")) {
            out.write("*2\r\n$4\r\npong\r\n$0\r\n\r\n".getBytes());
          } else if (command.equals("UNSUBSCRIBE")) {
            // handled above in the SUBSCRIBE/UNSUBSCRIBE branch — shouldn't reach here
            // but guard just in case
          } else {
            String lower = command.toLowerCase();
            out.write(("-ERR Can't execute '" + lower + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n").getBytes());
          }
        }  else if (command.equals("WATCH")) {
          if (inMulti) {
            out.write("-ERR WATCH inside MULTI is not allowed\r\n".getBytes());
          } else {
            for (int i = 1; i < parts.length; i++) {
              watchedKeys.add(parts[i]);
              keyDirtyFlags.computeIfAbsent(parts[i],
                  k -> Collections.synchronizedSet(new HashSet<>())).add(watchDirty);
            }
            out.write("+OK\r\n".getBytes());
          }
        } else if (command.equals("UNWATCH")) {
          for (String k : watchedKeys) {
            Set<AtomicBoolean> flags = keyDirtyFlags.get(k);
            if (flags != null) flags.remove(watchDirty);
          }
          watchedKeys.clear();
          watchDirty.set(false);
          out.write("+OK\r\n".getBytes());
          // existing auth + execCommand block
        } else {
          if (defaultUserNopass) authenticated = true;
          boolean isAuthExempt = command.equals("AUTH")
              || (command.equals("ACL") && parts.length >= 2
                  && parts[1].toUpperCase().equals("SETUSER"));
          if (!authenticated && !isAuthExempt) {
            out.write("-NOAUTH Authentication required.\r\n".getBytes());
            out.flush();
            continue;
          }
          if (command.equals("AUTH")) {
            String resp = execCommand(command, parts, out);
            if (resp.equals("+OK\r\n")) authenticated = true;
            out.write(resp.getBytes());
            out.flush();
            continue;
          }
          String resp = execCommand(command, parts, out);
          if (!resp.isEmpty()) out.write(resp.getBytes());
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
    } catch (Throwable e) {
      System.out.println("Exception in handleClient: " + e.getClass().getSimpleName() + ": " + e.getMessage());
    } finally {
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

      case "GEOSEARCH": {
        // GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit
        String key = parts[1];
        double centerLon = Double.parseDouble(parts[3]);
        double centerLat = Double.parseDouble(parts[4]);
        double radius    = Double.parseDouble(parts[6]);
        String unit      = parts[7].toLowerCase();
        // Convert radius to meters
        double radiusM;
        switch (unit) {
          case "km": radiusM = radius * 1000; break;
          case "mi": radiusM = radius * 1609.344; break;
          case "ft": radiusM = radius * 0.3048; break;
          default:   radiusM = radius; break; // "m"
        }
        TreeMap<String, Double> zset = zsetStore.get(key);
        List<String> matches = new ArrayList<>();
        if (zset != null) {
          for (Map.Entry<String, Double> e : zset.entrySet()) {
            double[] coords = geoDecodeScore((long) e.getValue().doubleValue());
            double dist = haversine(centerLon, centerLat, coords[0], coords[1]);
            if (dist <= radiusM) matches.add(e.getKey());
          }
        }
        StringBuilder sb = new StringBuilder("*" + matches.size() + "\r\n");
        for (String m : matches) sb.append("$").append(m.length()).append("\r\n").append(m).append("\r\n");
        return sb.toString();
      }

      case "GEODIST": {
        String key = parts[1];
        TreeMap<String, Double> zset = zsetStore.get(key);
        if (zset == null || !zset.containsKey(parts[2]) || !zset.containsKey(parts[3]))
          return "$-1\r\n";
        double[] c1 = geoDecodeScore((long) zset.get(parts[2]).doubleValue());
        double[] c2 = geoDecodeScore((long) zset.get(parts[3]).doubleValue());
        double dist = haversine(c1[0], c1[1], c2[0], c2[1]);
        String distStr = String.format("%.4f", dist);
        return "$" + distStr.length() + "\r\n" + distStr + "\r\n";
      }

      case "GEOPOS": {
        String key = parts[1];
        TreeMap<String, Double> zset = zsetStore.get(key);
        StringBuilder sb = new StringBuilder("*" + (parts.length - 2) + "\r\n");
        for (int i = 2; i < parts.length; i++) {
          if (zset == null || !zset.containsKey(parts[i])) {
            sb.append("*-1\r\n");
          } else {
            long score = (long) zset.get(parts[i]).doubleValue();
            double[] coords = geoDecodeScore(score);
            String lonStr = String.valueOf(coords[0]);
            String latStr = String.valueOf(coords[1]);
            sb.append("*2\r\n$").append(lonStr.length()).append("\r\n").append(lonStr).append("\r\n");
            sb.append("$").append(latStr.length()).append("\r\n").append(latStr).append("\r\n");
          }
        }
        return sb.toString();
      }

      case "GEOADD": {
        String key = parts[1];
        int added = 0;
        for (int i = 2; i + 2 < parts.length; i += 3) {
          double lon = Double.parseDouble(parts[i]);
          double lat = Double.parseDouble(parts[i + 1]);
          String member = parts[i + 2];
          if (lon < -180 || lon > 180)
            return "-ERR invalid longitude value " + lon + "\r\n";
          if (lat < -85.05112878 || lat > 85.05112878)
            return "-ERR invalid latitude value " + lat + "\r\n";
          TreeMap<String, Double> zset = zsetStore.computeIfAbsent(key, k -> new TreeMap<>());
          if (!zset.containsKey(member)) added++;
          zset.put(member, geoScore(lon, lat));
        }
        return ":" + added + "\r\n";
      }

      case "ZREM": {
        TreeMap<String, Double> zset = zsetStore.get(parts[1]);
        if (zset == null || !zset.containsKey(parts[2])) return ":0\r\n";
        zset.remove(parts[2]);
        return ":1\r\n";
      }

      case "ZSCORE": {
        TreeMap<String, Double> zset = zsetStore.get(parts[1]);
        if (zset == null || !zset.containsKey(parts[2])) return "$-1\r\n";
        double scoreVal = zset.get(parts[2]);
        String score;
        if (scoreVal == Math.floor(scoreVal) && !Double.isInfinite(scoreVal))
          score = String.valueOf((long) scoreVal);
        else
          score = String.valueOf(scoreVal);
        return "$" + score.length() + "\r\n" + score + "\r\n";
      }

      case "ZCARD": {
        TreeMap<String, Double> zset = zsetStore.get(parts[1]);
        return ":" + (zset == null ? 0 : zset.size()) + "\r\n";
      }

      case "ZRANGE": {
        String key = parts[1];
        int start = Integer.parseInt(parts[2]);
        int stop  = Integer.parseInt(parts[3]);
        TreeMap<String, Double> zset = zsetStore.get(key);
        if (zset == null) return "*0\r\n";
        List<Map.Entry<String, Double>> entries = new ArrayList<>(zset.entrySet());
        entries.sort((a, b) -> {
          int cmp = Double.compare(a.getValue(), b.getValue());
          return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
        });
        int size = entries.size();
        if (start < 0) start = Math.max(0, size + start);
        if (stop  < 0) stop  = size + stop;
        stop = Math.min(stop, size - 1);
        StringBuilder sb = new StringBuilder("*" + (stop - start + 1) + "\r\n");
        for (int i = start; i <= stop; i++) {
          String m = entries.get(i).getKey();
          sb.append("$").append(m.length()).append("\r\n").append(m).append("\r\n");
        }
        return sb.toString();
      }

      case "ZRANK": {
        String key = parts[1]; String member = parts[2];
        TreeMap<String, Double> zset = zsetStore.get(key);
        if (zset == null || !zset.containsKey(member)) return "$-1\r\n";
        // Sort by score, then lexicographically for equal scores
        List<Map.Entry<String, Double>> entries = new ArrayList<>(zset.entrySet());
        entries.sort((a, b) -> {
          int cmp = Double.compare(a.getValue(), b.getValue());
          return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
        });
        for (int i = 0; i < entries.size(); i++) {
          if (entries.get(i).getKey().equals(member)) return ":" + i + "\r\n";
        }
        return "$-1\r\n";
      }

      case "ZADD": {
        String key = parts[1];
        // parts: ZADD key score member [score member ...]
        zsetStore.putIfAbsent(key, new TreeMap<>());
        TreeMap<String, Double> zset = zsetStore.get(key);
        int added = 0;
        for (int i = 2; i + 1 < parts.length; i += 2) {
          double score = Double.parseDouble(parts[i]);
          String member = parts[i + 1];
          if (!zset.containsKey(member)) added++;
          zset.put(member, score);
        }
        return ":" + added + "\r\n";
      }

      case "PUBLISH": {
        String ch  = parts[1];
        String msg = parts[2];
        String payload = "*3\r\n$7\r\nmessage\r\n$" + ch.length() + "\r\n" + ch + "\r\n$"
                       + msg.length() + "\r\n" + msg + "\r\n";
        Set<OutputStream> subs = pubsubChannels.get(ch);
        int count = 0;
        if (subs != null) {
          synchronized (subs) {
            for (OutputStream sub : subs) {
              try { sub.write(payload.getBytes()); sub.flush(); count++; }
              catch (IOException ignored) {}
            }
          }
        }
        return ":" + count + "\r\n";
      }

      case "PING": return "+PONG\r\n";

      case "ECHO":
        return "$" + parts[1].length() + "\r\n" + parts[1] + "\r\n";

        case "SET":
          store.put(parts[1], parts[2]);
          if (parts.length >= 5 && parts[3].toUpperCase().equals("PX"))
            expiry.put(parts[1], System.currentTimeMillis() + Long.parseLong(parts[4]));
          else
            expiry.remove(parts[1]);
          markKeyDirty(parts[1]);
          if (aofStream != null) {
            try {
              StringBuilder aof = new StringBuilder("*" + parts.length + "\r\n");
              for (String p : parts) aof.append("$").append(p.length()).append("\r\n").append(p).append("\r\n");
              aofStream.write(aof.toString().getBytes());
              aofStream.flush();
            } catch (IOException ignored) {}
          }
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
        int needed   = Integer.parseInt(parts[1]);
        long timeout = Long.parseLong(parts[2]);
        if (masterOffset.get() == 0) return ":" + replicas.size() + "\r\n";
        byte[] getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".getBytes();
        for (ReplicaState rs : replicas) {
          try { rs.out.write(getack); rs.out.flush(); } catch (IOException ignored) {}
        }
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
          String value;
          switch (param) {
            case "dir":            value = dir; break;
            case "dbfilename":     value = dbfilename; break;
            case "appendonly":     value = appendonly; break;
            case "appenddirname":  value = appenddirname; break;
            case "appendfilename": value = appendfilename; break;
            case "appendfsync":    value = appendfsync; break;
            default:               value = ""; break;
          }
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

      case "ACL": {
        if (parts.length >= 2) {
          String sub = parts[1].toUpperCase();
          if (sub.equals("WHOAMI")) return "$7\r\ndefault\r\n";
          if (sub.equals("GETUSER") && parts.length >= 3 && parts[2].equals("default")) {
            // flags
            StringBuilder sb = new StringBuilder("*4\r\n$5\r\nflags\r\n");
            if (defaultUserNopass) {
              sb.append("*1\r\n$6\r\nnopass\r\n");
            } else {
              sb.append("*0\r\n");
            }
            // passwords
            sb.append("$9\r\npasswords\r\n*").append(defaultUserPasswords.size()).append("\r\n");
            for (String h : defaultUserPasswords)
              sb.append("$").append(h.length()).append("\r\n").append(h).append("\r\n");
            return sb.toString();
          }
          if (sub.equals("SETUSER") && parts.length >= 4 && parts[2].equals("default")) {
            for (int i = 3; i < parts.length; i++) {
              String rule = parts[i];
              if (rule.startsWith(">")) {
                String password = rule.substring(1);
                try {
                  java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
                  byte[] hash = md.digest(password.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                  StringBuilder hex = new StringBuilder();
                  for (byte b : hash) hex.append(String.format("%02x", b));
                  String hashStr = hex.toString();
                  if (!defaultUserPasswords.contains(hashStr)) defaultUserPasswords.add(hashStr);
                  defaultUserNopass = false;
                } catch (Exception e) {
                  return "-ERR hash error\r\n";
                }
              }
            }
            return "+OK\r\n";
          }
        }
        return "-ERR unknown ACL command\r\n";
      }

      case "AUTH": {
        // AUTH <username> <password>
        String username = parts.length >= 3 ? parts[1] : "default";
        String password = parts.length >= 3 ? parts[2] : parts[1];
        if (!username.equals("default"))
          return "-WRONGPASS invalid username-password pair or user is disabled.\r\n";
        if (defaultUserNopass) return "+OK\r\n";
        try {
          java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
          byte[] hash = md.digest(password.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          StringBuilder hex = new StringBuilder();
          for (byte b : hash) hex.append(String.format("%02x", b));
          String hashStr = hex.toString();
          if (defaultUserPasswords.contains(hashStr)) return "+OK\r\n";
        } catch (Exception ignored) {}
        return "-WRONGPASS invalid username-password pair or user is disabled.\r\n";
      }

      case "WATCH": return "+OK\r\n";

      default: return "-ERR unknown command\r\n";
    }  // end of switch
  }  // end of execCommand
    

  // ── Helpers ──────────────────────────────────────────────────────────────

  // Compute Redis geohash score: interleave 26 bits of lon and lat
  // Decode a geohash score back to [longitude, latitude]
  private static double haversine(double lon1, double lat1, double lon2, double lat2) {
    final double R = 6372797.560856; // Earth radius in meters (Redis value)
    double dLat = Math.toRadians(lat2 - lat1);
    double dLon = Math.toRadians(lon2 - lon1);
    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
             + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
             * Math.sin(dLon / 2) * Math.sin(dLon / 2);
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  private static double[] geoDecodeScore(long score) {
    long lonBits = 0, latBits = 0;
    for (int i = 0; i < 26; i++) {
      latBits |= ((score >> (2 * i))     & 1L) << i; // even bits → lat
      lonBits |= ((score >> (2 * i + 1)) & 1L) << i; // odd bits  → lon
    }
    double lon = ((lonBits + 0.5) / (double)(1L << 26)) * 360.0 - 180.0;
    double lat = ((latBits + 0.5) / (double)(1L << 26)) * 170.10225756 - 85.05112878;
    return new double[]{lon, lat};
  }

  private static double geoScore(double lon, double lat) {
    double normLon = (lon + 180.0) / 360.0;
    double normLat = (lat + 85.05112878) / 170.10225756;
    long lonBits = Math.min((1L << 26) - 1, (long)Math.floor(normLon * (1L << 26)));
    long latBits = Math.min((1L << 26) - 1, (long)Math.floor(normLat * (1L << 26)));
    long score = 0;
    for (int i = 0; i < 26; i++) {
      score |= ((latBits >> i) & 1L) << (2 * i);
      score |= ((lonBits >> i) & 1L) << (2 * i + 1);
    }
    return (double) score;
  }

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

  private static void markKeyDirty(String key) {
    Set<AtomicBoolean> flags = keyDirtyFlags.get(key);
    if (flags != null) synchronized (flags) {
      for (AtomicBoolean f : flags) f.set(true);
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

  private static void replayAof(String path) {
    try (InputStream in = new FileInputStream(path)) {
      String line;
      while ((line = readLineRaw(in)) != null) {
        if (!line.startsWith("*")) continue;
        int numArgs = Integer.parseInt(line.substring(1));
        String[] parts = new String[numArgs];
        for (int i = 0; i < numArgs; i++) {
          readLineRaw(in);          // skip $len line
          parts[i] = readLineRaw(in);
        }
        if (parts[0] != null) {
          try { execCommand(parts[0].toUpperCase(), parts, null); } catch (Exception ignored) {}
        }
      }
    } catch (Exception e) {
      System.out.println("AOF replay error: " + e.getMessage());
    }
  }

  // ── RDB Loader ───────────────────────────────────────────────────────────

  private static void loadRdb(String path) {
    try (DataInputStream dis = new DataInputStream(new FileInputStream(path))) {
      byte[] magic = new byte[9];
      dis.readFully(magic);

      while (true) {
        int op = dis.read();
        if (op == -1 || op == 0xFF) break;

        if (op == 0xFA) { readRdbString(dis); readRdbString(dis); continue; }
        if (op == 0xFE) { readRdbLength(dis); continue; }
        if (op == 0xFB) { readRdbLength(dis); readRdbLength(dis); continue; }

        long expireMs = -1;
        int valueType = op;

        if (op == 0xFC) { expireMs = readLongLE(dis); valueType = dis.read(); }
        else if (op == 0xFD) { expireMs = readIntLE(dis) * 1000L; valueType = dis.read(); }

        if (valueType == 0) {
          String key = readRdbString(dis);
          String val = readRdbString(dis);
          store.put(key, val);
          if (expireMs > 0) expiry.put(key, expireMs);
        } else {
          break;
        }
      }
    } catch (FileNotFoundException ignored) {
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
    return -(first & 0x3F);
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