package sbp.util;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TransactionDao {

    public static List<Transaction> getTransactionsInPeriod() {
        List<Transaction> result = new ArrayList<>();
        try {
            Class.forName("org.h2.Driver");
            try (
                    Connection conn = DriverManager.getConnection("jdbc:h2:~/transactions");
                    Statement stat = conn.createStatement()
            ) {
                try(ResultSet rs = stat.executeQuery("select * from transactionKafka WHERE Timestamp >= now() - interval 10 minute")) {
                    while (rs.next()) {
                        result.add(
                                new Transaction(
                                        rs.getLong("id"),
                                        rs.getString("transactionJson"),
                                        rs.getTimestamp("transactionDate"))
                        );
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }catch (ClassNotFoundException exception) {
            throw new RuntimeException(exception);
        }
        return result;
    }
}
