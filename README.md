# Random Dataset Generator

Generates random datasets onto a Kafka topic to be picked up by the DB loader.

Usage:

First, make sure Kafka and the DB loader are running. Check `src/main/resources/kafka.properties` is correct.

```bash
mvn clean compile exec:java
```

## Customising the dimensions

You can control which dimensions are in the dataset by passing command-line arguments. Each argument is the name of
a properties file under `src/main/resources` that contains CSV rows for that dimension. The dataset generator will randomly
pick a row from each dimension file to combine into each row in the generated dataset.


