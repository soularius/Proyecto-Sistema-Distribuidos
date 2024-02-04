/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.pruebadistribuido;

/**
 *
 * @author ADMIN
 */

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterServer {
    private static final int PORT = 12345;
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(4);
    private static final ConcurrentHashMap<Socket, PrintWriter> clients = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, String> taskStatus = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> taskData = new ConcurrentHashMap<>();
    private static final Map<Socket, Integer> clientTaskMap = new ConcurrentHashMap<>();
    private static final AtomicInteger nextTaskId = new AtomicInteger(0);
    private static int numWorkers;

    public static void main(String[] args) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Master Server is running on port " + PORT);
            
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("\n** Configuración del Servidor **\n");
            System.out.print("- Ingrese el número de clientes en ejecución: ");
            numWorkers = Integer.parseInt(br.readLine());
            System.out.println("\n____________\n");
            System.out.print("- Ingrese el número de secuencia de Lucas para calcular: ");
            int sequenceNumber = Integer.parseInt(br.readLine());
            System.out.println("** CALCULANDO **");

            for (int i = 0; i <= sequenceNumber; i++) {
                taskData.put(nextTaskId.get(), i);
                taskStatus.put(nextTaskId.getAndIncrement(), "pending");
            }

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    threadPool.execute(new ClientHandler(clientSocket, numWorkers));
                } catch (IOException e) {
                    System.out.println("Se detectó una excepción al intentar escuchar en el puerto " + PORT + " o escuchando una conexión");
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private int numWorkers;

        public ClientHandler(Socket socket, int numWorkers) {
            this.clientSocket = socket;
            this.numWorkers = numWorkers;
        }

        @Override
        public void run() {
            try (
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));
            ) {
                clients.put(clientSocket, out);

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    if (inputLine.startsWith("RESULT:")) {
                        handleResult(inputLine, clientSocket);
                    } else if (inputLine.equals("TASK_REQUEST")) {
                        dispatchPendingTasks(out, clientSocket);
                    }
                }
            } catch (IOException e) {
                System.out.println("Se detectó una excepción al intentar escuchar en el puerto "
                        + PORT + " o escuchando una conexión");
                System.out.println(e.getMessage());
            } finally {
                handleClientFailure(clientSocket);
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.out.println("Se detectó una excepción al intentar escuchar en el puerto "
                        + PORT + " o escuchando una conexión");
                    System.out.println(e.getMessage());
                }
                clients.remove(clientSocket);
            }
        }
    }

    private static void assignTask(PrintWriter out, int taskId) {
        int n = taskData.get(taskId);
        taskStatus.put(taskId, "assigned");
        out.println("TASK:" + taskId + ":" + n);
    }

    private static void handleResult(String inputLine, Socket clientSocket) {
        String[] parts = inputLine.split(":");
        int taskId = Integer.parseInt(parts[1]);
        int result = Integer.parseInt(parts[2]);

        taskStatus.put(taskId, "completed");
        System.out.println("Proceso " + taskId + " completado con el resultado: " + result);

        clientTaskMap.remove(clientSocket);

        PrintWriter out = clients.get(clientSocket);
        if (out != null) {
            dispatchPendingTasks(out, clientSocket);
        }
        checkCompletionAndRequestNewSequence();
    }

    private static void redistributeTask(Integer taskId) {
        if (taskStatus.get(taskId).equals("assigned")) {
            for (Map.Entry<Socket, PrintWriter> client : clients.entrySet()) {
                if (!client.getValue().equals(taskStatus.get(taskId))) {
                    PrintWriter out = client.getValue();
                    int n = taskData.get(taskId);
                    out.println("TASK:" + taskId + ":" + n);
                    taskStatus.put(taskId, "assigned");
                    System.out.println("Tarea " + taskId + " Ha sido redistribuida a otra cliente.");
                    break;
                }
            }
        }
    }

    private static synchronized void handleClientFailure(Socket clientSocket) {
        clients.remove(clientSocket);
        taskStatus.entrySet().stream()
            .filter(entry -> "assigned".equals(entry.getValue()))
            .forEach(entry -> {
                PrintWriter clientOut = clients.get(clientSocket);
                if (clientOut != null && clientOut.equals(clients.get(clientSocket))) {
                    entry.setValue("pending");
                    redistributeTask(entry.getKey());
                }
            });
    }

    private static synchronized void dispatchPendingTasks(PrintWriter out, Socket clientSocket) {
        Integer assignedTaskId = clientTaskMap.get(clientSocket);
        if (assignedTaskId == null) {
            taskStatus.entrySet().stream()
                .filter(entry -> "pending".equals(entry.getValue()))
                .findFirst()
                .ifPresent(taskEntry -> {
                    assignTask(out, taskEntry.getKey());
                    clientTaskMap.put(clientSocket, taskEntry.getKey());
                });
        }
    }

    private static void checkCompletionAndRequestNewSequence() {
        boolean allTasksCompleted = taskStatus.values().stream()
            .allMatch(status -> "completed".equals(status));

        if (allTasksCompleted) {
            System.out.println("-- Todas las tareas completadas. --");

            // Ejecutar la solicitud de una nueva secuencia en un hilo separado para no bloquear el hilo principal
            threadPool.submit(() -> {
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                    System.out.print("Ingrese un nuevo número de secuencia de Lucas para calcular (0 para salir): ");
                    int newSequenceNumber = Integer.parseInt(br.readLine());

                    if (newSequenceNumber == 0) {
                        System.out.println("Salir del programa según lo solicitado por el usuario.");
                        shutdownServer();
                    } else {
                        resetTasks(newSequenceNumber);

                        // Despachar las nuevas tareas a los clientes
                        clients.forEach((socket, out) -> dispatchPendingTasks(out, socket));
                    }
                } catch (IOException e) {
                    System.out.println("Error al leer la entrada del usuario para un nuevo número de secuencia.");
                    e.printStackTrace();
                }
            });
        }
    }

    private static void resetTasks(int sequenceNumber) {
        taskData.clear();
        taskStatus.clear();
        nextTaskId.set(0);
        clientTaskMap.clear();

        for (int i = 0; i <= sequenceNumber; i++) {
            taskData.put(nextTaskId.get(), i);
            taskStatus.put(nextTaskId.getAndIncrement(), "pending");
        }
    }

    private static void shutdownServer() {
        try {
            System.out.println("Apagar el servidor.");
            threadPool.shutdown();
            clients.forEach((socket, out) -> {
                out.close();
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println("Error al cerrar el socket del cliente.");
                    e.printStackTrace();
                }
            });
            System.exit(0);
        } catch (Exception e) {
            System.out.println("Se produjo un error al cerrar el servidor..");
            e.printStackTrace();
        }
    }
}