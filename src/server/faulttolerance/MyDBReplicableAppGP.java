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

/**
* MyDBReplicableAppGP
*
* This class implements a GigaPaxos Replicable application that fronts
* a Cassandra-backed key-value / event-log table.
*
* Each replica uses its own Cassandra keyspace (e.g., "server0", "server1"),
* and within that keyspace we store a single table:
*
* grade(id int PRIMARY KEY, events list<int>)
*
* All client requests are SQL strings (INSERT / UPDATE) that operate on
* this table. GigaPaxos ensures the same totally-ordered sequence of
* requests is applied at all replicas. This class is responsible for:
*
* - Parsing the request from a RequestPacket.
* - Qualifying the table name with the replica's keyspace.
* - Executing the SQL on Cassandra.
* - Providing checkpoint() and restore() so the RSM can recover state.
*/
public class MyDBReplicableAppGP implements Replicable {

    /** Sleep constant used in some tests (kept for consistency). */
    public static final int SLEEP = 1000;

    /** Name of the Cassandra table we operate on. */
    private static final String DEFAULT_TABLE_NAME = "grade";

    /** Cassandra cluster handle shared by this application instance. */
    private Cluster cluster;

    /** Cassandra session used to execute queries. */
    private Session session;

    /**
    * Logical keyspace name for this replica, e.g., "server0", "server1".
    * The constructor receives it as args[0].
    */
    private final String keyspace;

    /**
    * Cached PreparedStatement for restoring entries.
    * 
    */
    private PreparedStatement insertPrepared;

    /** Prepared statement used during restore() to insert rows efficiently. */
    private PreparedStatement restorePrepared;

    /**
    * Constructor: initializes Cassandra connection, creates the keyspace and
    * grade table if they do not already exist, and prepares statements.
    *
    * @param args expects args[0] to be the keyspace name for this replica.
    * @throws IOException if initialization fails in an unrecoverable way.
    */
    public MyDBReplicableAppGP(String[] args) throws IOException {
        // Defensive check: keyspace must be provided and non-empty.
        if (args == null || args.length == 0 || args[0] == null || args[0].trim().isEmpty()) {
            throw new IllegalArgumentException("Expect args[0] = keyspace");
        }
        this.keyspace = args[0].trim();

        // Connect to local Cassandra cluster (default 127.0.0.1:9042).
     
        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect();

        // Try to create keyspace with SimpleStrategy replication.
        // This is idempotent; if someone else already created it, this is a no-op.
        try {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};");
        } catch (Exception e) {
            System.err.println("Warning: could not create keyspace " + keyspace + " : " + e);
        }

        // Try to create the grade table inside this keyspace.
        // Every replica has the same schema; state is separated by keyspace.
        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + DEFAULT_TABLE_NAME
                    + " (id int PRIMARY KEY, events list<int>);");
        } catch (Exception e) {
            System.err.println("Warning: could not ensure table " + keyspace + "." + DEFAULT_TABLE_NAME + " : " + e);
        }

        // Prepare statements once during initialization.
        // These are used for both normal inserts (if desired) and restore().
        this.insertPrepared = session.prepare(
                "INSERT INTO " + keyspace + "." + DEFAULT_TABLE_NAME + " (id, events) VALUES (?, ?);"
        );
        this.restorePrepared = session.prepare(
                "INSERT INTO " + keyspace + "." + DEFAULT_TABLE_NAME + " (id, events) VALUES (?, ?);"
        );

        System.out.println("MyDBReplicableAppGP initialized for keyspace: " + keyspace);
    }

    /**
    * Helper to qualify the shared table name with this replica's keyspace.
    *
    * The tests issue SQL against table "grade", but the actual Cassandra
    * table is "<keyspace>.grade", e.g., "server0.grade".
    *
    * This method:
    * - returns the SQL unchanged if it already contains "<keyspace>.grade"
    * - otherwise, replaces bare "grade" with "<keyspace>.grade"
    *
    * @param sql Original SQL statement, possibly unqualified.
    * @return SQL with the table name qualified for this keyspace.
    */
    private String maybeQualifyKeyspace(String sql) {
        if (sql == null) return null;

        // If already qualified with this replica's keyspace, do nothing.
        if (sql.contains(keyspace + "." + DEFAULT_TABLE_NAME)) {
            return sql;
        }

        // Replace all standalone "grade" tokens with "<keyspace>.grade".
        // The word boundary in the regex avoids touching substrings.
        String qualified = sql.replaceAll("\\b" + DEFAULT_TABLE_NAME + "\\b",
                keyspace + "." + DEFAULT_TABLE_NAME);

        System.out.println("Qualified: " + sql + " -> " + qualified);
        return qualified;
    }

    /**
    * Overload of execute() required by the Replicable interface that also
    * receives a doNotReplyToClient flag. This implementation simply delegates
    * to the core execute(Request) method, ignoring the flag because the
    * application itself does not manage replies.
    */
    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        // The underlying logic is the same regardless of whether the caller wants a reply.
        return execute(request);
    }

    /**
    * Core method that GigaPaxos calls to apply a state machine command.
    *
    * Behavior:
    * 1. Extract the SQL string from the Request (ideally a RequestPacket).
    * 2. If requestValue is null, fall back to parsing the JSON "QV" field
    *    from request.toString().
    * 3. Qualify the table name with this replica's keyspace.
    * 4. Execute the SQL on Cassandra using session.execute().
    *
    * If SQL cannot be extracted, the method logs an error and returns false.
    * Otherwise it returns true on success. If an exception is thrown, it is
    * logged and the method returns false.
    *
    * Per GigaPaxos documentation, execute() is expected to be atomic
    * and to return true only when the request has been handled (or safely
    * discarded). Returning false can cause retries or stalls.
    */
    @Override
    public boolean execute(Request request) {
        try {
            String sql = null;

            // Preferred path: Request is a RequestPacket with a requestValue.
            // This is the normal case when GigaPaxos carries the SQL as the value.
            if (request instanceof RequestPacket) {
                RequestPacket rp = (RequestPacket) request;
                Object reqValue = rp.requestValue;
                if (reqValue != null) {
                    sql = reqValue.toString();
                }
            }

            // Fallback: parse the JSON string for field "QV" (the request value).
            // This helps in scenarios where the requestValue field is not set explicitly.
            if (sql == null || sql.trim().isEmpty()) {
                String jsonStr = request.toString();
                int qvStart = jsonStr.indexOf("\"QV\":\"");
                if (qvStart != -1) {
                    qvStart += 6; // move past '"QV":"'
                    int qvEnd = jsonStr.indexOf("\"", qvStart);
                    if (qvEnd > qvStart) {
                        sql = jsonStr.substring(qvStart, qvEnd);
                    }
                }
            }

            // If we still don't have a valid SQL string, we consider this a failure.
            // Returning false signals to GigaPaxos that the command was not applied.
            if (sql == null || sql.trim().isEmpty()) {
                System.err.println("Could not extract SQL from: " + request);
                return false;
            }

            // Qualify the grade table with this keyspace.
            String qualifiedSql = maybeQualifyKeyspace(sql.trim());

            System.out.println("Executing: " + qualifiedSql);

            // Execute the SQL command directly. We rely on Cassandra to handle
            // inserts, updates, and list operations like events = events + [x].
            session.execute(qualifiedSql);

            System.out.println("✓ Success");
            return true;

        } catch (Exception e) {
            System.err.println("execute() failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
    * checkpoint() is called by GigaPaxos to obtain a compact snapshot of the
    * current application state. We implement it by:
    *
    * 1. Reading all rows from <keyspace>.grade.
    * 2. Creating a JSON object mapping:
    *    id (as string) -> list of event integers
    * 3. If the events column is null, we store an empty list [] instead.
    *
    * The returned JSON string is stored by the RSM and can later be passed
    * to restore() during recovery.
    */
    @Override
    public String checkpoint(String name) {
        try {
            System.out.println("CHECKPOINT called for " + keyspace);

            // Capture the full logical state of the table at this replica.
            ResultSet rs = session.execute(
                    "SELECT id, events FROM " + keyspace + "." + DEFAULT_TABLE_NAME + ";");

            org.json.JSONObject root = new org.json.JSONObject();

            int rowCount = 0;
            for (Row r : rs) {
                int id = r.getInt("id");

                // events may be null in Cassandra; treat that as empty list.
                List<Integer> events = r.getList("events", Integer.class);
                if (events == null) {
                    events = new ArrayList<>();
                }

                // Store each row as id -> events[]
                root.put(String.valueOf(id), events);
                rowCount++;
            }

            String checkpointState = root.toString();
            System.out.println("✓ Checkpoint completed: " + rowCount +
                    " rows, size=" + checkpointState.length() + " chars");

            return checkpointState;
        } catch (Exception e) {
            System.err.println("checkpoint() failed: " + e.getMessage());
            e.printStackTrace();
            // Returning an empty JSON object is a safe fallback; state can still be rebuilt via log.
            return "{}";
        }
    }

    /**
    * restore() is called by GigaPaxos during recovery, or when a replica is
    * caught up from a snapshot. It should:
    *
    * 1. Clear existing state for this application.
    * 2. Reconstruct the table contents from the provided JSON snapshot.
    *
    * Our approach:
    * - TRUNCATE <keyspace>.grade.
    * - Parse the JSON into a mapping id -> events list.
    * - For each entry, we reconstruct the list<int> and insert with a
    *   cached prepared statement.
    *
    * If the state is empty ("{}" or blank), we simply truncate and return true.
    */
    @Override
    public boolean restore(String name, String state) {
        try {
            System.out.println("RESTORE called for " + keyspace + " with state length="
                    + (state != null ? state.length() : 0));

            // Clear existing rows for this replica's table before restoring.
            // This ensures we do not mix old and new state.
            session.execute("TRUNCATE " + keyspace + "." + DEFAULT_TABLE_NAME + ";");

            // Handle the case of an empty snapshot gracefully.
            if (state == null || state.trim().isEmpty() || state.trim().equals("{}")) {
                System.out.println("✓ Restore completed: empty state");
                return true;
            }

            org.json.JSONObject root = new org.json.JSONObject(state);

            // root.names() gives a JSONArray of the keys (string ids).
            org.json.JSONArray keys = root.names();
            if (keys == null) {
                System.out.println("✓ Restore completed: no keys");
                return true;
            }

            // Rebuild each row from its JSON representation.
            for (int i = 0; i < keys.length(); i++) {
                String key = keys.getString(i);
                int id = Integer.parseInt(key);

                // Value may be null or a JSONArray, depending on what checkpoint wrote.
                Object value = root.get(key);

                List<Integer> events = new ArrayList<>();

                // If we have a JSON array, convert it to List<Integer>.
                if (value != null && value instanceof org.json.JSONArray) {
                    org.json.JSONArray arr = (org.json.JSONArray) value;
                    for (int j = 0; j < arr.length(); j++) {
                        events.add(arr.getInt(j));
                    }
                }
                // If value is null or not a JSONArray, we treat events as empty.

                System.out.println("Restoring: id=" + id + ", events=" + events +
                        " (size=" + events.size() + ")");

                // Use cached prepared statement to insert the row.
                session.execute(restorePrepared.bind(id, events));
            }

            System.out.println("✓ Restore completed for keyspace: " + keyspace +
                    ", restored " + keys.length() + " keys");

            return true;

        } catch (Exception e) {
            System.err.println("restore() failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
    * Construct a Request object from a JSON string. This is required by the
    * Replicable interface so that GigaPaxos can reconstruct user requests
    * from their serialized form.
    */
    @Override
    public Request getRequest(String s) throws RequestParseException {
        try {
            // RequestPacket understands the JSON format used by GigaPaxos.
            return new RequestPacket(new org.json.JSONObject(s));
        } catch (Exception e) {
            // Wrap any parsing error in a RequestParseException as required by the interface.
            throw new RequestParseException(e);
        }
    }

    /**
    * Return the set of IntegerPacketTypes supported by this application.
    */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        // No application-specific packet types; all logic goes through RequestPacket.
        return new HashSet<IntegerPacketType>();
    }
}