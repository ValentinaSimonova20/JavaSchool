package sbp;

import org.junit.jupiter.api.Test;
import sbp.checkhash.CalculateHashSumScheduler;

public class HashSumCalculationTest {

    @Test
    void success() {
        while (true) {
            CalculateHashSumScheduler.schedule();
        }
    }
}
