package net.elodina.mesos.util;

import java.io.IOError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Net {
    public static int findAvailPort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public static boolean isPortAvail(String host, int port) {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(host, port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static boolean isPortOpen(String host, int port) {
        try (Socket socket = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
