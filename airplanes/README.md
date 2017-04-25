## Airplanes

Queries on the following dataset:

https://catalog.data.gov/dataset/aircraft-tail-numbers-and-models-at-sfo

Query 1:

```
SELECT COUNT(*) FROM airplanes WHERE
    AircraftModel == "B747-400" AND
    Airline == "United"
```
