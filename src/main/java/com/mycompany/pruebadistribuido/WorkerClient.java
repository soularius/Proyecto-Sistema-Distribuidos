/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.pruebadistribuido;

/**
 *
 * @author ADMIN
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class WorkerClient {
    private static final String HOST = "localhost";
    private static final int PORT = 12345;
    private static Map<Integer, Integer> lucasCache = new HashMap<>();

    public static void main(String[] args) {
        try (
            Socket socket = new Socket(HOST, PORT);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))
        ) {
            System.out.println("Conectado al servidor maestro");

            out.println("TASK_REQUEST");

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.startsWith("TASK:")) {
                    String[] parts = inputLine.split(":");
                    int taskId = Integer.parseInt(parts[1]);
                    int n = Integer.parseInt(parts[2]);
                    int lucasNum = lucasNumber(n);
                    out.println("RESULT:" + taskId + ":" + lucasNum);
                }
            }
        } catch (Exception e) {
            System.out.println("Se detectó una excepción al intentar conectarse a " +
                HOST + " on port " + PORT);
            System.out.println(e.getMessage());
        }
    }

    private static int lucasNumber(int n) {
        if (n == 0) return 2;
        if (n == 1) return 1;
        if (lucasCache.containsKey(n)) return lucasCache.get(n);

        int result = lucasNumber(n - 1) + lucasNumber(n - 2);
        lucasCache.put(n, result);
        return result;
    }
}