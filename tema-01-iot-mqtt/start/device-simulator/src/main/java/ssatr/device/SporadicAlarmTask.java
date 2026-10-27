package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * TASK SPORADIC: tratează alarmele de temperatură.
 *
 * Evenimentele sosesc în coada alarmEvents (puse de taskul periodic).
 * Intervalul minim între sosiri este MIN_INTERARRIVAL_MS: între două eliberări
 * ale taskului (două alarme publicate) trebuie să treacă cel puțin atât.
 * Intervalul este impus de cod: evenimentele care sosesc mai des sunt
 * respinse, ca să nu fie inundat brokerul.
 */
public class SporadicAlarmTask implements Runnable {

    static final long MIN_INTERARRIVAL_MS = 5000;

    private final String deviceId;
    private final MqttConnection mqtt;
    private final BlockingQueue<Double> alarmEvents;
    private final PeriodicTelemetryTask periodic;

    public SporadicAlarmTask(
            String deviceId,
            MqttConnection mqtt,
            BlockingQueue<Double> alarmEvents,
            PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        long lastRelease = 0;

        while(!Thread.currentThread().isInterrupted())
        {
            try
            {
                var temperature = alarmEvents.take();

                var now = System.currentTimeMillis();
                var elapsedTime = now - lastRelease;

                if(elapsedTime < MIN_INTERARRIVAL_MS)
                {
                    System.out.printf("Device %s rejected alarm: only %d ms since last alarm (minimum %d ms)%n", deviceId, elapsedTime, MIN_INTERARRIVAL_MS);
                    continue;
                }

                lastRelease = now;

                var messageToSend = String.format(Locale.US, "%s;%d;%.2f;%.2f", deviceId, now, temperature, periodic.getThreshold());
                var topic = Config.alarmTopic(deviceId);
                mqtt.publish(topic, messageToSend);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
