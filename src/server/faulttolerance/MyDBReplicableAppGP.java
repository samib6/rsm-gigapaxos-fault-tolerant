package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;
    private static final String DEFAULT_TABLE_NAME = "grade";

    private Cluster cluster;
    private Session session;
    private final String keyspace;
    
    // *CRITICAL: Cache prepared statements*
    private PreparedStatement insertPrepared;
    private PreparedStatement restorePrepared;

    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0 || args[0] == null || args[0].trim().isEmpty()) {
            throw new IllegalArgumentException("Expect args[0] = keyspace");
        }
        this.keyspace = args[0].trim();

        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect();

        try {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};");
        } catch (Exception e) {
            System.err.println("Warning: could not create keyspace " + keyspace + " : " + e);
        }

        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + DEFAULT_TABLE_NAME
                    + " (id int PRIMARY KEY, events list<int>);");
        } catch (Exception e) {
            System.err.println("Warning: could not ensure table " + keyspace + "." + DEFAULT_TABLE_NAME + " : " + e);
        }
        
        // *CRITICAL: Prepare statements once during initialization*
        this.insertPrepared = session.prepare(
            "INSERT INTO " + keyspace + "." + DEFAULT_TABLE_NAME + " (id, events) VALUES (?, ?);"
        );
        this.restorePrepared = session.prepare(
            "INSERT INTO " + keyspace + "." + DEFAULT_TABLE_NAME + " (id, events) VALUES (?, ?);"
        );
        
        System.out.println("MyDBReplicableAppGP initialized for keyspace: " + keyspace);
    }

    private String maybeQualifyKeyspace(String sql) {
        if (sql == null) return null;
        
        if (sql.contains(keyspace + "." + DEFAULT_TABLE_NAME)) {
            return sql;
        }
        
        String qualified = sql.replaceAll("\\b" + DEFAULT_TABLE_NAME + "\\b", 
                                           keyspace + "." + DEFAULT_TABLE_NAME);
        
        System.out.println("Qualified: " + sql + " -> " + qualified);
        return qualified;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return execute(request);
    }

    @Override
public boolean execute(Request request) {
    try {
        String sql = null;
        
        if (request instanceof RequestPacket) {
            RequestPacket rp = (RequestPacket) request;
            Object reqValue = rp.requestValue;
            if (reqValue != null) {
                sql = reqValue.toString();
            }
        }
        
        if (sql == null || sql.trim().isEmpty()) {
            String jsonStr = request.toString();
            int qvStart = jsonStr.indexOf("\"QV\":\"");
            if (qvStart != -1) {
                qvStart += 6;
                int qvEnd = jsonStr.indexOf("\"", qvStart);
                if (qvEnd > qvStart) {
                    sql = jsonStr.substring(qvStart, qvEnd);
                }
            }
        }
        
        if (sql == null || sql.trim().isEmpty()) {
            System.err.println("Could not extract SQL from: " + request);
            return false;
        }
        
        String qualifiedSql = maybeQualifyKeyspace(sql.trim());
        
        System.out.println("Executing: " + qualifiedSql);
        
        // Just execute everything normally - no special handling
        session.execute(qualifiedSql);
        
        System.out.println("✓ Success");
        return true;
        
    } catch (Exception e) {
        System.err.println("execute() failed: " + e.getMessage());
        e.printStackTrace();
        return false;
    }
}
    @Override
    public String checkpoint(String name) {
        try {
            System.out.println("CHECKPOINT called for " + keyspace);
            
            ResultSet rs = session.execute("SELECT id, events FROM " + keyspace + "." + DEFAULT_TABLE_NAME + ";");

            org.json.JSONObject root = new org.json.JSONObject();

            int rowCount = 0;
            for (Row r : rs) {
                int id = r.getInt("id");
                
                // Handle null events column
                List<Integer> events = r.getList("events", Integer.class);
                
                if (events == null) {
                    events = new ArrayList<>();
                }
                
                root.put(String.valueOf(id), events);
                rowCount++;
            }
            
            String checkpointState = root.toString();
            System.out.println("✓ Checkpoint completed: " + rowCount + " rows, size=" + checkpointState.length() + " chars");
            
            return checkpointState;
        } catch (Exception e) {
            System.err.println("checkpoint() failed: " + e.getMessage());
            e.printStackTrace();
            return "{}";
        }
    }

    @Override
    public boolean restore(String name, String state) {
        try {
            System.out.println("RESTORE called for " + keyspace + " with state length=" + 
                             (state != null ? state.length() : 0));
            
            session.execute("TRUNCATE " + keyspace + "." + DEFAULT_TABLE_NAME + ";");

            if (state == null || state.trim().isEmpty() || state.trim().equals("{}")) {
                System.out.println("✓ Restore completed: empty state");
                return true;
            }

            org.json.JSONObject root = new org.json.JSONObject(state);

            var keys = root.names();
            if (keys == null) {
                System.out.println("✓ Restore completed: no keys");
                return true;
            }

            for (int i = 0; i < keys.length(); i++) {
                String key = keys.getString(i);

                int id = Integer.parseInt(key);
                
                // Check if the value is null or JSONArray
                Object value = root.get(key);
                
                List<Integer> events = new ArrayList<>();
                
                if (value != null && value instanceof org.json.JSONArray) {
                    org.json.JSONArray arr = (org.json.JSONArray) value;
                    for (int j = 0; j < arr.length(); j++) {
                        events.add(arr.getInt(j));
                    }
                }

                System.out.println("Restoring: id=" + id + ", events=" + events + " (size=" + events.size() + ")");
                
                // Use CACHED prepared statement
                session.execute(restorePrepared.bind(id, events));
            }

            System.out.println("✓ Restore completed for keyspace: " + keyspace + ", restored " + keys.length() + " keys");
            
            return true;

        } catch (Exception e) {
            System.err.println("restore() failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Request getRequest(String s) throws RequestParseException {
        try {
            return new RequestPacket(new org.json.JSONObject(s));
        } catch (Exception e) {
            throw new RequestParseException(e);
        }
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>();
    }
}