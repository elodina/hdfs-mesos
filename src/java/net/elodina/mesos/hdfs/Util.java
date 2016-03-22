package net.elodina.mesos.hdfs;

import java.io.IOError;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Util {
    public static int findAvailPort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IOError(e);
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
