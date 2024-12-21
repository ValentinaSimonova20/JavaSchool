package sbp.school.kafka;

public class Main {
    public static void main(String[] args) {
        ProducerService producerService = new ProducerService();
        producerService.send();
    }
}
