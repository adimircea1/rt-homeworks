package ssatr.service;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * Preia mesajele de un anumit tip ("telemetry", "alarm" sau "status") din coada
 * inbox, le salvează și actualizează dashboard-ul.
 *
 * Formatul mesajelor (câmpuri separate prin ";"):
 *   telemetry:  deviceId;timestamp;temperatura
 *   alarm:      deviceId;timestamp;temperatura;prag
 *   status:     deviceId;timestamp;text
 */
public class MessageProcessor implements Runnable {

    private final String kind;
    private final BlockingQueue<String> inbox;
    private final CsvStorage storage;
    private final Dashboard dashboard;

    public MessageProcessor(String kind, BlockingQueue<String> inbox, CsvStorage storage, Dashboard dashboard) {
        this.kind = kind;
        this.inbox = inbox;
        this.storage = storage;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted())
        {
            try
            {
                var message = inbox.take().trim();

                if(message.isEmpty())
                {
                    continue;
                }

                try
                {
                    ProcessMessage(message);
                }
                catch (IllegalArgumentException e)
                {
                    System.out.printf("[%s] invalid message \"%s\": %s%n", kind, message, e.getMessage());
                }
            }
            catch(InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void ProcessMessage(String message)
    {
        switch (kind)
        {
            case "telemetry" ->
            {
                var fields = SplitFields(message, 3);
                var deviceId = fields[0];
                var timestamp = Long.parseLong(fields[1]);
                var temperature = Double.parseDouble(fields[2]);

                storage.append(kind, message);
                dashboard.UpdateTelemetry(deviceId, timestamp, temperature);
            }
            case "alarm" ->
            {
                var fields = SplitFields(message, 4);
                var deviceId = fields[0];
                var timestamp = Long.parseLong(fields[1]);
                var temperature = Double.parseDouble(fields[2]);
                var threshold = Double.parseDouble(fields[3]);

                storage.append(kind, message);
                dashboard.RegisterAlarm(deviceId, timestamp, temperature, threshold);
                System.out.printf(Locale.US, "ALARM %s: temperature %.2f exceeded threshold %.2f%n", deviceId, temperature, threshold);
            }
            case "status" ->
            {
                var fields = SplitFields(message, 3);
                var deviceId = fields[0];
                var timestamp = Long.parseLong(fields[1]);
                var text = fields[2];

                storage.append(kind, message);
                dashboard.UpdateStatus(deviceId, timestamp, text);
                System.out.printf("STATUS %s: %s%n", deviceId, text);
            }
            default -> throw new IllegalArgumentException(String.format("unknown message kind: %s", kind));
        }
    }

    private static String[] SplitFields(String message, int expectedFieldsCount)
    {
        var fields = message.split(";", expectedFieldsCount);

        if (fields.length != expectedFieldsCount)
        {
            throw new IllegalArgumentException(String.format("expected %d fields, got %d", expectedFieldsCount, fields.length));
        }

        if (fields[0].isBlank())
        {
            throw new IllegalArgumentException("empty deviceId");
        }

        return fields;
    }
}
