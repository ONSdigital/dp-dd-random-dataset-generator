package uk.co.onsdigital.discovery.gendata;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Generates random datasets of the given size to the given Kafka producer.
 */
@Slf4j
public class RandomDatasetGenerator implements Runnable, Closeable {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final long numRows;
    private final KafkaProducer<String, String> producer;
    private final String datasetId;
    private final String filename;
    private final Map<String, List<String>> dimensionMap = new ConcurrentHashMap<>();

    private RandomDatasetGenerator(long numRows, String...dimensions) throws IOException {
        this.numRows = numRows;
        this.datasetId = UUID.randomUUID().toString();
        this.filename = RandomUtils.randomString("test", ".csv");

        Properties props = new Properties();
        props.load(getClass().getResourceAsStream("/kafka.properties"));

        log.info("Loading dimensions: {}", Arrays.toString(dimensions));
        for (String dimension : dimensions) {
            dimensionMap.put(dimension, lines(dimension).collect(toList()));
        }

        log.info("Connecting to Kafka: {}", props);
        this.producer = new KafkaProducer<>(props);
        log.info("Connected to Kafka");
    }

    public void run() {
        log.info("Starting dataset {} filename {} numRows {}", datasetId, filename, numRows);
        try {
            for (long i = 0; i < numRows; ++i) {
                val csvRecord = CsvRecord.builder().datasetID(datasetId).s3URL("s3://dummy/" + filename).index(i).row(buildCsvRow()).build();
                log.info("Sending row {}", csvRecord);
                producer.send(new ProducerRecord<>("test", jsonMapper.writeValueAsString(csvRecord)));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private Stream<String> lines(String file) {
        return new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/" + file + ".csv"))).lines();
    }

    private String buildCsvRow() {
        // 36929,"","","","","","",2014,Year,,NACE,08 - Other mining and quarrying,Prodcom Elements,UK manufacturer sales LABEL
        val year = RandomUtils.randomYear();
        return csv(RandomUtils.randomLong(), "", "", "", "", "", "", year, "Year", "")
                + "," + dimensions();
    }

    private String dimensions() {
        return dimensionMap.values().stream().map(RandomUtils::randomElement).collect(joining(","));
    }

    private String csv(Object...cells) {
        return Arrays.stream(cells).map(x -> x == null ? "" : ("\"" + x + "\"")).collect(Collectors.joining(","));
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    public static void main(String...args) throws IOException {
        String[] dimensions = args;
        if (dimensions.length == 0) {
            dimensions = new String[] { "Sex", "NACE", "Prodcom Elements"};
        }
        try (val datasetGenerate = new RandomDatasetGenerator(1000L, dimensions)) {
            datasetGenerate.run();
        }
    }
}
