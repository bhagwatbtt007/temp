import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CisamToHive {

    public static void main(String[] args) {
        String hiveUrl = "jdbc:hive2://your-hive-server:10000";
        String hiveUser = "your-hive-user";
        String hivePassword = "your-hive-password";
        String cisamFilePath = "/path/to/your/cisam/data.dat";
        String hiveTableName = "your_hive_table";

        try (Connection connection = DriverManager.getConnection(hiveUrl, hiveUser, hivePassword);
             Statement statement = connection.createStatement();
             BufferedReader reader = new BufferedReader(new FileReader(cisamFilePath))) {

            // Create Hive table if it doesn't exist (example)
            statement.execute("CREATE TABLE IF NOT EXISTS " + hiveTableName + " (col1 STRING, col2 INT)");

            String line;
            while ((line = reader.readLine()) != null) {
                // Parse C-ISAM data (replace with actual parsing logic)
                String[] values = parseCisamLine(line);

                // Construct Hive INSERT statement
                String insertSql = "INSERT INTO " + hiveTableName + " VALUES ('" + values[0] + "', " + values[1] + ")";
                statement.execute(insertSql);
            }

            System.out.println("Data inserted into Hive successfully.");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    // Replace with your C-ISAM parsing logic
    private static String[] parseCisamLine(String line) {
        // ... C-ISAM parsing logic ...
        // Example (replace with your actual logic):
        return line.split(",");
    }
}



import java.sql.*;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONObject;

public class DynamicInsertWithOrgJson {
    public static void main(String[] args) {
        String jsonInput = """
        {
            "tablename": "item",
            "items": [
                { "itemId": 1, "product": "Pen", "quantity": 2 },
                { "itemId": 2, "product": "Notebook", "quantity": 1 }
            ]
        }
        """;

        String dbUrl = "jdbc:oracle:thin:@localhost:1521:xe";
        String user = "your_username";
        String pass = "your_password";

        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass)) {
            conn.setAutoCommit(false);

            JSONObject root = new JSONObject(jsonInput);
            String tableName = root.getString("tablename");
            JSONArray items = root.getJSONArray("items");

            if (items.isEmpty()) {
                System.err.println("No items to insert.");
                return;
            }

            // Use the first item to get column names
            JSONObject firstItem = items.getJSONObject(0);
            Iterator<String> keys = firstItem.keys();

            StringBuilder columns = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            while (keys.hasNext()) {
                String key = keys.next();
                columns.append(key).append(",");
                placeholders.append("?").append(",");
            }

            // Remove trailing commas
            columns.setLength(columns.length() - 1);
            placeholders.setLength(placeholders.length() - 1);

            String insertSQL = "INSERT INTO " + tableName +
                    " (" + columns + ") VALUES (" + placeholders + ")";
            System.out.println("Generated SQL: " + insertSQL);

            try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                for (int i = 0; i < items.length(); i++) {
                    JSONObject item = items.getJSONObject(i);
                    int index = 1;
                    for (String key : firstItem.keySet()) {
                        Object value = item.opt(key);
                        stmt.setObject(index++, value);
                    }
                    stmt.addBatch();
                }

                stmt.executeBatch();
                conn.commit();
                System.out.println("All items inserted into " + tableName + " in one commit.");
            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

