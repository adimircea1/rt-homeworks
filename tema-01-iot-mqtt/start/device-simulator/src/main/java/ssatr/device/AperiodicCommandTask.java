package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * TASK APERIODIC: execută comenzile primite de la business service.
 *
 * Comenzile sosesc oricând, pe MQTT, fără un interval minim între ele, și sunt
 * puse în coada commands. Fiecare comandă eliberează un job al taskului.
 * Comenzi care trebuie suportate:
 *
 *   SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
 *   SET_THRESHOLD <valoare> schimbă pragul de alarmă
 *   RESET                   readuce senzorul la valoarea inițială
 *   STATUS                  raportează perioada și pragul curente
 *
 * După executarea fiecărei comenzi, device-ul publică pe Config.statusTopic(deviceId)
 * o confirmare: "deviceId;timestamp;text", de exemplu
 * "device-1;1727600000000;OK SET_INTERVAL 1000".
 */
public class AperiodicCommandTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<String> commands;
    private final PeriodicTelemetryTask periodic;

    public AperiodicCommandTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                BlockingQueue<String> commands, PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.commands = commands;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted())
        {
            try
            {
                var command = commands.take();
                var commandSplit = command.trim().split("\\s+");
                var commandName = commandSplit[0].toUpperCase();

                var reply = "";
                try
                {
                    switch (commandName)
                    {
                        case "SET_INTERVAL" ->
                        {
                            if (commandSplit.length != 2)
                            {
                                throw new IllegalArgumentException(String.format("%s needs 1 parameter", commandName));
                            }

                            var convertedValue = Long.parseLong(commandSplit[1]);

                            if (convertedValue <= 0)
                            {
                                throw new IllegalArgumentException(String.format("%s value must be > 0", commandName));
                            }

                            periodic.setPeriodMs(convertedValue);
                            reply = String.format("OK %s %d", commandName, convertedValue);
                        }
                        case "SET_THRESHOLD" ->
                        {
                            if (commandSplit.length != 2)
                            {
                                throw new IllegalArgumentException(String.format("%s needs 1 parameter", commandName));
                            }

                            var convertedValue = Double.parseDouble(commandSplit[1]);

                            if (Double.isNaN(convertedValue) || Double.isInfinite(convertedValue))
                            {
                                throw new IllegalArgumentException(String.format("%s value must be a finite number", commandName));
                            }

                            periodic.setThreshold(convertedValue);
                            reply = String.format(Locale.US, "OK %s %.2f", commandName, convertedValue);
                        }
                        case "RESET" ->
                        {
                            if (commandSplit.length != 1)
                            {
                                throw new IllegalArgumentException(String.format("%s takes no parameters", commandName));
                            }

                            sensor.reset();
                            reply = String.format("OK %s", commandName);
                        }
                        case "STATUS" ->
                        {
                            if (commandSplit.length != 1)
                            {
                                throw new IllegalArgumentException(String.format("%s takes no parameters", commandName));
                            }

                            reply = String.format(Locale.US, "OK %s period=%d threshold=%.2f", commandName, periodic.getPeriodMs(), periodic.getThreshold());
                        }
                        default ->
                        {
                            throw new IllegalArgumentException(String.format("unknown command: %s", commandSplit[0]));
                        }
                    }
                }
                catch (NumberFormatException e)
                {
                    reply = String.format("ERROR invalid number: %s", commandSplit[1]);
                }
                catch (IllegalArgumentException e)
                {
                    reply = String.format("ERROR %s", e.getMessage());
                }

                var timestamp = System.currentTimeMillis();
                var message = String.format("%s;%d;%s", deviceId, timestamp, reply);
                var topic = Config.statusTopic(deviceId);

                mqtt.publish(topic, message);
            }
            catch(InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
