package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * TASK PERIODIC, cu perioada T = periodMs: fiecare job citește senzorul și
 * publică valoarea pe topic-ul de telemetrie.
 *
 * Dacă temperatura depășește pragul, taskul NU tratează el alarma, ci doar
 * semnalează un eveniment în coada alarmEvents. Alarma este tratată de
 * taskul sporadic.
 */
public class PeriodicTelemetryTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<Double> alarmEvents;

    // volatile: sunt citite aici, dar modificate de taskul aperiodic (alt thread)
    private volatile long periodMs = 2000;
    private volatile double threshold = 28.0;

    public PeriodicTelemetryTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                 BlockingQueue<Double> alarmEvents) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
    }

    @Override
    public void run() {

        //r_0
        var nextRunTime = System.nanoTime();

        while(!Thread.currentThread().isInterrupted())
        {
            var periodMsFinal = periodMs;
            var finalThreshold = threshold;

            var temperature = sensor.read();
            var timestamp = System.currentTimeMillis();
            var messageToSend = String.format(Locale.US, "%s;%d;%.2f", deviceId, timestamp, temperature);
            var topic = Config.telemetryTopic(deviceId);
            mqtt.publish(topic, messageToSend);

            if(temperature > finalThreshold)
            {
                var enqueued = alarmEvents.offer(temperature);

                if(!enqueued)
                {
                    System.out.printf(Locale.US, "Device %s alarm queue is full, event dropped. Read temperature %.2f%n", deviceId, temperature);
                }
            }

            //r_(k+1) = r_k + T
            nextRunTime += periodMsFinal * 1_000_000;
            var remainingTime = nextRunTime - System.nanoTime();

            if(remainingTime > 0)
            {
                try
                {
                    Thread.sleep(remainingTime / 1_000_000, (int)(remainingTime % 1_000_000));
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            else
            {
                nextRunTime = System.nanoTime();
            }
        }
    }

    public void setPeriodMs(long periodMs) {
        this.periodMs = periodMs;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getPeriodMs() {
        return periodMs;
    }

    public double getThreshold() {
        return threshold;
    }
}
