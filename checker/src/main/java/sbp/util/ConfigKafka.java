package sbp.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
                "check-topic",
                config.getString("check-topic")
        );

        return properties;
    }

    public static Properties getConsumerConfig(String groupId) {

        PropertiesConfiguration config = new PropertiesConfiguration();
        try {
            config.load("application.properties");
        } catch (ConfigurationException exception) {
            System.out.println("Не удалось загрузить конфигурационный файл: " + exception.getMessage());
            throw new RuntimeException(exception);
        }

        Properties properties = new Properties();


        properties.put(
                "check-topic",
                config.getString("check-topic")
        );
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
        );
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                config.getString(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
        );
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                config.getString(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
        );
        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                config.getString(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
        );

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }
}
