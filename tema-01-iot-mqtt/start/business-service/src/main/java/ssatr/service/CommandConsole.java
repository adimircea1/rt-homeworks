package ssatr.service;

import java.util.Scanner;

/**
 * Consola operatorului: citește comenzi de la tastatură.
 *
 *   show                               afișează dashboard-ul
 *   <deviceId> <COMANDA> [parametru]   trimite o comandă unui device, de exemplu
 *                                      device-1 SET_INTERVAL 1000
 *   help                               afișează comenzile
 *   exit                               oprește serviciul
 */
public class CommandConsole implements Runnable {

    private final MqttConnection mqtt;
    private final Dashboard dashboard;

    public CommandConsole(MqttConnection mqtt, Dashboard dashboard) {
        this.mqtt = mqtt;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        Scanner scanner = new Scanner(System.in);
        printHelp();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();

            if (line.isEmpty()) {
                continue;
            } else if (line.equals("exit")) {
                break;
            } else if (line.equals("help")) {
                printHelp();
            } else if (line.equals("show")) {
                dashboard.print();
            } else {
                sendCommand(line);
            }
        }
    }

    private void sendCommand(String line) {
        var parts = line.split("\\s+", 2);

        if (parts.length != 2)
        {
            System.out.printf("Invalid format: \"%s\". Expected: <deviceId> <COMMAND> [parameter]%n", line);
            return;
        }

        var deviceId = parts[0];
        var command = parts[1];

        if (deviceId.contains("/") || deviceId.contains("+") || deviceId.contains("#"))
        {
            System.out.printf("Invalid deviceId: \"%s\" (must not contain '/', '+' or '#')%n", deviceId);
            return;
        }

        mqtt.publish(Config.commandTopic(deviceId), command);
        System.out.printf("Sent \"%s\" to %s%n", command, deviceId);
    }

    private void printHelp() {
        System.out.println("Comenzi: show | <deviceId> SET_INTERVAL <ms> | <deviceId> SET_THRESHOLD <valoare>");
        System.out.println("         <deviceId> RESET | <deviceId> STATUS | help | exit");
    }
}
