## Airplanes

This directory contains queries on the on the following dataset:

https://catalog.data.gov/dataset/aircraft-tail-numbers-and-models-at-sfo

Query 0:

```
Scan Input without parsing
```

Query 1:

```
SELECT COUNT(*) FROM airplanes WHERE
    AircraftModel == "B747-400" AND
    Airline == "United Airlines"
```

Query 2:

```
SELECT COUNT(*) FROM airplanes WHERE
    Airline CONTAINS "United" OR
    Airline CONTAINS "Delta" OR
    Airline CONTAINS "American"
```
