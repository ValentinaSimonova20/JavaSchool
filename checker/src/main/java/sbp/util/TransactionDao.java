package sbp.util;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TransactionDao {

    public static List<Transaction> getTransactionsInPeriod(Timestamp timestamp, int timestampForCheckMinutes) {
        List<Transaction> result = new ArrayList<>();
        try {
            Class.forName("org.h2.Driver");
            try (
                    Connection conn = DriverManager.getConnection("jdbc:h2:~/transactions");
                    PreparedStatement prepStat = conn.prepareStatement(
                            "select * from transactionKafka WHERE transactionDate " +
                                    ">= cast (? as timestamp) - cast (? as interval minute)"
                    )
            ) {
                prepStat.setTimestamp(1, timestamp);
                prepStat.setInt(2, timestampForCheckMinutes);
                prepStat.execute();
                try(ResultSet rs = prepStat.getResultSet()) {
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
