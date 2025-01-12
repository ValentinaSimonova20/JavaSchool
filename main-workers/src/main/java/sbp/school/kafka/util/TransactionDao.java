package sbp.school.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class TransactionDao {

    public static void createTable() {
        try {
            Class.forName("org.h2.Driver");
            try (
                    Connection conn = DriverManager.getConnection("jdbc:h2:~/transactions");
                    Statement stat = conn.createStatement()
            ) {
                stat.execute("create table IF NOT EXISTS transactionKafka(id integer not null auto_increment, transactionJson varchar(255), transactionDate TIMESTAMP)");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }catch (ClassNotFoundException exception) {
            throw new RuntimeException(exception);
        }
    }

    public static void saveTransaction(Transaction transaction) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
        objectMapper.registerModule(new JavaTimeModule());
        try {
            Class.forName("org.h2.Driver");
            try (
                    Connection conn = DriverManager.getConnection("jdbc:h2:~/transactions");
                    PreparedStatement stat = conn.prepareStatement("INSERT INTO transactionKafka(transactionJson, transactionDate) VALUES (?,?)")
            ) {
                stat.setString(1, objectMapper.writeValueAsString(transaction));
                stat.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                stat.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }catch (ClassNotFoundException exception) {
            throw new RuntimeException(exception);
        }
    }

    public static List<String> getAllTransactions() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
        objectMapper.registerModule(new JavaTimeModule());
        List<String> result = new ArrayList<>();
        try {
            Class.forName("org.h2.Driver");
            try (
                    Connection conn = DriverManager.getConnection("jdbc:h2:~/transactions");
                    Statement stat = conn.createStatement()
            ) {
                try(ResultSet rs = stat.executeQuery("select * from transactionKafka")) {
                    while (rs.next()) {
                        result.add(rs.getString("transactionJson"));
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
