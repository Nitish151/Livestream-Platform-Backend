import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TestConn {
    public static void main(String[] args) {
        String url = "jdbc:clickhouse://localhost:8123/default";
        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", "");
        
        System.out.println("Testing connection to: " + url);
        try {
            // Load driver
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            try (Connection conn = DriverManager.getConnection(url, props)) {
                System.out.println("Success!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        url = "jdbc:ch://localhost:8123/default";
        System.out.println("\nTesting connection to: " + url);
        try {
            try (Connection conn = DriverManager.getConnection(url, props)) {
                System.out.println("Success!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
