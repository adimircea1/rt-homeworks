package ssatr.service;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Ultima stare cunoscută a fiecărui device: ultima temperatură, momentul
 * ultimului mesaj, numărul de alarme etc.
 *
 * Este actualizată de thread-urile MessageProcessor și citită de CommandConsole,
 * deci accesul trebuie sincronizat.
 */

public class Dashboard {
    private static class DeviceState
    {
        double lastTemperature = Double.NaN;
        long lastMessageTime;
        int alarmCount;
        String lastStatus = "-";
    }

    private final Map<String, DeviceState> devices = new TreeMap<>();

    public synchronized void UpdateTelemetry(String deviceId, long timestamp, double temperature)
    {
        var state = GetOrCreate(deviceId);
        state.lastTemperature = temperature;
        state.lastMessageTime = Math.max(state.lastMessageTime, timestamp);
    }

    public synchronized void RegisterAlarm(String deviceId, long timestamp, double temperature, double threshold)
    {
        var state = GetOrCreate(deviceId);
        state.alarmCount++;
        state.lastMessageTime = Math.max(state.lastMessageTime, timestamp);

        if (Double.isNaN(state.lastTemperature))
        {
            state.lastTemperature = temperature;
        }
    }

    public synchronized void UpdateStatus(String deviceId, long timestamp, String text)
    {
        var state = GetOrCreate(deviceId);
        state.lastStatus = text;
        state.lastMessageTime = Math.max(state.lastMessageTime, timestamp);
    }

    /** Afișează un tabel cu starea tuturor device-urilor. */
    public synchronized void print()
    {
        if (devices.isEmpty())
        {
            System.out.println("(no devices yet)");
            return;
        }

        var now = System.currentTimeMillis();

        System.out.printf("%-12s %-8s %-15s %-7s %s%n", "DEVICE", "TEMP", "LAST MESSAGE", "ALARMS", "LAST STATUS");

        for (var entry : devices.entrySet())
        {
            var state = entry.getValue();
            var temperature = Double.isNaN(state.lastTemperature) ? "-" : String.format(Locale.US, "%.2f", state.lastTemperature);
            var ageSeconds = Math.max(0, (now - state.lastMessageTime) / 1000);
            var age = String.format("%d s ago", ageSeconds);

            System.out.printf("%-12s %-8s %-15s %-7d %s%n", entry.getKey(), temperature, age, state.alarmCount, state.lastStatus);
        }
    }

    private DeviceState GetOrCreate(String deviceId)
    {
        return devices.computeIfAbsent(deviceId, id -> new DeviceState());
    }
}
