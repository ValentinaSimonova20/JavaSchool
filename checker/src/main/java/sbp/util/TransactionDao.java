package sbp.util;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class TransactionDao {

    private static final Logger logger = Logger.getLogger(TransactionDao.class.getName());

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
                logger.severe("Произошла ошибка при работе с базой данных: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }catch (ClassNotFoundException exception) {
            logger.severe("Не нашли класс org.h2.Driver. сообщение: " + exception.getMessage());
            throw new RuntimeException(exception);
        }
        return result;
    }

    public static int getTransactionsHashSumInPeriod(Timestamp timestamp, int timestampForCheckMinutes) {
        int result = 0;
        try {
            Class.forName("org.h2.Driver");
            try (
                    Connection conn = DriverManager.getConnection("jdbc:h2:~/transactionsConsumer");
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
                        result += Long.valueOf(rs.getLong("id")).hashCode();
                    }
                }
            } catch (Exception e) {
                logger.severe("Произошла ошибка при работе с базой данных: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }catch (ClassNotFoundException exception) {
            logger.severe("Не нашли класс org.h2.Driver. сообщение: " + exception.getMessage());
            throw new RuntimeException(exception);
        }
        return result;
    }
}
