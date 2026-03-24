# Data Science Exercise: Cleaning and Analyzing Weather Data with Java Streams

## Overview

In real data science work, raw data is often messy. Before you can compute averages, compare cities, or look for patterns, you usually need to:

- parse raw text into structured records
- remove malformed rows
- reject impossible values
- group and summarize the cleaned data

In this exercise, you will use **Java Streams** to process a weather dataset modeled after public NOAA-style daily weather data.

The CSV file is:

`noaa_weather_sample_200_rows.csv`

Your starter code is in:

`WeatherDataScienceExercise.java`

---

## Dataset columns

Each row in the CSV represents one weather observation:

- `stationId`
- `city`
- `date`
- `temperatureC`
- `humidity`
- `precipitationMm`

Some rows are intentionally bad so you can practice cleaning data.

Examples of bad data in this file include:

- malformed rows
- missing temperature
- impossible temperatures
- invalid humidity
- negative precipitation

---

## Learning goals

This exercise practices the kinds of data transformations that are common in data science:

- parsing raw rows
- filtering invalid data
- transforming data into records
- grouping and partitioning
- computing aggregate statistics

You should use Java Streams as much as possible.

---

## Your tasks

### Part 1 — Parse the raw data

Implement:

```java
static Optional<WeatherRecord> parseRow(String row)
```

Requirements:

1. Split the row by commas.
2. Reject malformed rows.
3. Reject rows with missing temperature.
4. Safely parse numeric values.
5. Return `Optional.empty()` if parsing fails.

This is your **raw data -> structured data** step.

---

### Part 2 — Clean the data

Implement:

```java
static boolean isValid(WeatherRecord r)
```

Keep only rows where:

- temperature is between `-60` and `60`
- humidity is between `0` and `100`
- precipitation is greater than or equal to `0`

This is your **data cleaning** step.

---

### Part 3 — Analyze the cleaned data

After you build the `cleaned` list, complete the following tasks.

#### Task 1
Count how many valid weather records remain after cleaning.

#### Task 2
Compute the average temperature across all valid rows.

#### Task 3
Find the city with the highest average temperature.

#### Task 4
Group records by city.

#### Task 5
Compute average precipitation by city.

#### Task 6
Partition rows into:

- freezing days: temperature `<= 0`
- non-freezing days: temperature `> 0`

#### Task 7
Create a `Set<String>` of all distinct cities.

#### Task 8
Find the wettest single day.

#### Task 9
Create a `Map<String, Double>` from city to average humidity.

#### Task 10
Produce a list of formatted strings like:

```text
Miami on 2025-01-02: 25.1C, humidity 82%
```

#### Task 11 (optional)
Build a summary object for each city using:

```java
record CityWeatherSummary(
    String city,
    long dayCount,
    double avgTemp,
    double avgPrecipitation,
    double maxTemp
) {}
```

Create:

```java
Map<String, CityWeatherSummary>
```

---

## Part 4 — Unit Tests

Write unit tests for your implementation in a new test class:

```
src/test/java/edu/touro/las/mcon364/streams/ds/WeatherDataScienceExerciseTest.java
```

### Required tests

#### `parseRow` tests
- A well-formed row returns a non-empty `Optional` with correct field values.
- A row with too few columns returns `Optional.empty()`.
- A row with a missing temperature value returns `Optional.empty()`.
- A row with a non-numeric temperature returns `Optional.empty()`.

#### `isValid` tests
- A record with temperature `60` is valid (boundary).
- A record with temperature `-60` is valid (boundary).
- A record with temperature `61` is invalid.
- A record with temperature `-61` is invalid.
- A record with humidity `0` is valid (boundary).
- A record with humidity `100` is valid (boundary).
- A record with humidity `-1` is invalid.
- A record with humidity `101` is invalid.
- A record with negative precipitation is invalid.
- A record with precipitation `0` is valid.

#### Integration tests (using the real CSV)
- After parsing and cleaning, the cleaned list is non-empty.
- All records in the cleaned list pass `isValid`.
- The city with the highest average temperature is a non-null, non-empty string.
- The wettest single day has precipitation `>= 0`.

### Guidelines
- Use **JUnit 5** (`@Test`, `assertNotNull`, `assertTrue`, `assertEquals`, `assertFalse`, `assertThrows`, etc.).
- Each test method should test **one thing**.
- Use descriptive method names, e.g. `parseRow_missingTemperature_returnsEmpty`.
- Do not rely on `System.out` output in tests — assert on return values and collections directly.

---

## Suggested Stream operations

You will likely use many of these:

- `map`
- `filter`
- `flatMap`
- `toList`
- `collect`
- `groupingBy`
- `partitioningBy`
- `averagingDouble`
- `summarizingDouble`
- `toSet`
- `toMap`
- `max`
- `count`

A common pattern in this exercise is:

```java
rows.stream()
    .map(...)
    .flatMap(...)
    .filter(...)
    .collect(...);
```

---

## Suggested workflow

A good order to work in:

1. Implement `parseRow`
2. Implement `isValid`
3. Confirm that `cleaned` contains only valid rows
4. Solve the analysis tasks one by one
5. Refactor repeated logic into helper methods if needed

---

## Hints

### Hint 1
Use:

```java
.flatMap(Optional::stream)
```

after mapping each row to `Optional<WeatherRecord>`.

### Hint 2
For average temperature by city, you may want:

```java
Collectors.groupingBy(..., Collectors.averagingDouble(...))
```

### Hint 3
For the wettest single day, think about:

```java
.max(Comparator.comparingDouble(...))
```

### Hint 4
For partitioning freezing vs non-freezing days, use:

```java
Collectors.partitioningBy(...)
```

---

## Real-world connection

This is a miniature version of a real data science workflow:

**CSV rows -> parse -> clean -> transform -> summarize -> insights**

That same pattern appears in analytics, machine learning preprocessing, public health data work, finance, and scientific data processing.

---

## Deliverables

Complete `WeatherDataScienceExercise.java` so that it:

- parses the CSV file
- cleans invalid rows
- computes the required summaries
- prints readable output

Write `MyExerciseTest.java` so that it:

- covers all required test cases listed in Part 4
- all tests pass with `mvn test`

---

## Stretch ideas

If you finish early, try one of these:

- convert Celsius to Fahrenheit in the formatted output
- sort cities by average temperature descending
- list the top 3 wettest days
- count how many rows were rejected and explain why
- export a cleaned CSV file

