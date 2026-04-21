package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.*;


public class WeatherDataScienceExercise {

    record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        // TODO 1:
        // Count how many valid weather records remain after cleaning.
        cleaned.stream().count();

        // TODO 2:
        // Compute the average temperature across all valid rows.
        cleaned.stream().map(t->t.temperatureC).collect(Collectors.averagingDouble(t->t));
        // TODO 3:
        // Find the city with the highest average temperature.
        var cityAndHighestTemp = cleaned.stream().collect(Collectors.groupingBy(
                w->w.city,Collectors.averagingDouble(w->w.temperatureC)));
        cityAndHighestTemp.entrySet().stream().max(Comparator.comparingDouble(Map.Entry::getValue))
                .map(Map.Entry::getKey).ifPresent(System.out::println);
        // TODO 4:
        // Group records by city.
        cleaned.stream().collect(Collectors.groupingBy(w->w.city));

        // TODO 5:
        // Compute average precipitation by city.
        cleaned.stream().collect(Collectors.groupingBy(w->w.city,
                Collectors.averagingDouble(w->w.precipitationMm)));
        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).
        cleaned.stream().collect(Collectors.partitioningBy(w->w.temperatureC<= 0));
        // TODO 7:
        // Create a Set<String> of all distinct cities.
        cleaned.stream().map(w->w.city).distinct().collect(Collectors.toSet());
        // TODO 8:
        // Find the wettest single day.
        cleaned.stream().max(Comparator.comparingDouble(w->w.precipitationMm)).
                map(w->w.date).ifPresent(System.out::println);
        // TODO 9:
        // Create a Map<String, Double> from city to average humidity.
        cleaned.stream().collect(Collectors.groupingBy(w->w.city,
                        Collectors.averagingDouble(w->w.humidity)));

        // TODO 10:
        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"
        cleaned.stream().collect(Collectors.toList());
        // TODO 11 (optional):
        // Build a Map<String, CityWeatherSummary> for all cities.

        // Put your code below these comments or refactor into helper methods.
    }

    static Optional<WeatherRecord> parseRow(String row) {
        // TODO:
        // 1. Split the row by commas
        // 2. Reject malformed rows
        // 3. Reject rows with missing temperature
        // 4. Parse numeric values safely
        // 5. Return Optional.empty() if parsing fails
        if(row == null) return Optional.empty();
        String[] parsedRow = row.split(",");
        if (!(parsedRow.length ==6)) {
            return Optional.empty();
        }else if ((parsedRow[3].isBlank())){
            return Optional.empty();
        }
        try {
            return Optional.of(
                    new WeatherRecord(
                            parsedRow[0].trim(),
                            parsedRow[1].trim(),
                            parsedRow[2].trim(),
                            Double.parseDouble(parsedRow[3].trim()),
                            Integer.parseInt(parsedRow[4].trim()),
                            Double.parseDouble(parsedRow[5].trim())
                    )
            );
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    static boolean isValid(WeatherRecord r) {
        // TODO:
        // Keep only rows where:
        // - temperature is between -60 and 60
        // - humidity is between 0 and 100
        // - precipitation is >= 0
        boolean valid = true;
        if(!(r.temperatureC <= 60 && r.temperatureC >= -60)){
            valid = false;
        }else if(!(r.humidity <= 100 && r.humidity >= 0)){
            valid = false;
        }else if(!(r.precipitationMm >= 0)){
            valid = false;
        }
        return valid;
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    private static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }
}
