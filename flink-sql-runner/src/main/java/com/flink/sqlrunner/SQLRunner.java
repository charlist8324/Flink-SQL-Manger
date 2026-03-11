package com.flink.sqlrunner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Flink SQL Runner
 * Executes SQL files in a Flink cluster
 */
public class SQLRunner {

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String sqlFilePath = null;
        String jobName = "flink-sql-job";
        int parallelism = 1;

        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--sqlFile") && i + 1 < args.length) {
                sqlFilePath = args[++i];
            } else if (args[i].equals("--jobName") && i + 1 < args.length) {
                jobName = args[++i];
            } else if (args[i].equals("--parallelism") && i + 1 < args.length) {
                parallelism = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--help")) {
                printUsage();
                System.exit(0);
            }
        }

        if (sqlFilePath == null) {
            System.err.println("Error: --sqlFile parameter is required");
            printUsage();
            System.exit(1);
        }

        try {
            executeSQL(sqlFilePath, jobName, parallelism);
        } catch (Exception e) {
            System.err.println("Error executing SQL: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void executeSQL(String sqlFilePath, String jobName, int parallelism)
            throws IOException {

        // Read SQL file
        String sqlContent = new String(Files.readAllBytes(Paths.get(sqlFilePath)));
        
        System.out.println("=== Flink SQL Runner ===");
        System.out.println("SQL File: " + sqlFilePath);
        System.out.println("Job Name: " + jobName);
        System.out.println("Parallelism: " + parallelism);
        System.out.println("========================");

        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        
        // Create table environment
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Set job name
        tableEnv.getConfig().set("pipeline.name", jobName);

        // Split SQL by semicolon and execute each statement
        String[] sqlStatements = sqlContent.split(";");

        for (String sql : sqlStatements) {
            String trimmedSQL = sql.trim();
            if (trimmedSQL.isEmpty()) {
                continue;
            }

            System.out.println("\nExecuting SQL:");
            System.out.println(trimmedSQL);
            System.out.println("----------------");

            try {
                // Execute SQL statement
                TableResult result = tableEnv.executeSql(trimmedSQL);
                
                // If it's an INSERT statement, the job will be submitted
                // For SELECT or other queries, print the result
                if (trimmedSQL.toUpperCase().startsWith("SELECT")) {
                    // Print query results
                    System.out.println("Query Result:");
                    result.print();
                } else {
                    // For other statements (CREATE, INSERT, etc.)
                    // Wait for the job to complete if it's an INSERT
                    if (trimmedSQL.toUpperCase().startsWith("INSERT") || 
                        trimmedSQL.toUpperCase().startsWith("CREATE TABLE AS")) {
                        System.out.println("Job submitted successfully. Waiting for completion...");
                        // This will block until the job finishes
                        result.await();
                        System.out.println("Job completed successfully!");
                    } else {
                        System.out.println("Statement executed successfully!");
                    }
                }
            } catch (Exception e) {
                System.err.println("Error executing statement: " + e.getMessage());
                throw e;
            }
        }
    }

    private static void printUsage() {
        System.out.println("Flink SQL Runner - Executes SQL files in Flink cluster");
        System.out.println("");
        System.out.println("Usage: flink run -c com.flink.sqlrunner.SQLRunner flink-sql-runner.jar --sqlFile <file> [options]");
        System.out.println("");
        System.out.println("Options:");
        System.out.println("  --sqlFile <path>    SQL file to execute (required)");
        System.out.println("  --jobName <name>     Job name (default: flink-sql-job)");
        System.out.println("  --parallelism <num>  Job parallelism (default: 1)");
        System.out.println("  --help               Show this help message");
        System.out.println("");
        System.out.println("Examples:");
        System.out.println("  flink run -c com.flink.sqlrunner.SQLRunner flink-sql-runner.jar \\");
        System.out.println("    --sqlFile /path/to/job.sql --jobName MyJob --parallelism 2");
        System.out.println("");
    }
}
