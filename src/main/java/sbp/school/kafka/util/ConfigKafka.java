package sbp.school.kafka.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class ConfigKafka {

    public static Properties getKafkaProperties(){
        PropertiesConfiguration config = new PropertiesConfiguration();
        try {
            config.load("application.properties");
        } catch (ConfigurationException exception) {
            System.out.println("Не удалось загрузить конфигурационный файл: " + exception.getMessage());
            throw new RuntimeException(exception);
        }

        Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
        );
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                config.getString(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
        );
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                config.getString(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
        );
        properties.put(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                config.getString(ProducerConfig.PARTITIONER_CLASS_CONFIG)
        );
        properties.put(
                "topic",
                config.getString("topic")
        );
        return properties;
    }
}
