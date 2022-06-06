import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mskrasilnikova on 27.02.20.
 */
public class TestBigFile {
    private static Logger log = LoggerFactory
            .getLogger(TestBigFile.class);

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        TestBigFile app = new TestBigFile();
        app.start();
    }

    /**
     * The processing code.
     */


    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Excel to Dataframe")
                .master("local")
                .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());

        Map<String, String> options = new HashMap<>();
        options.put("header", "true");
        options.put("inferSchema", "true");
        options.put("dataAddress", "'Sheet1'!B7:M16");
        options.put("maxRowsInMemory", "200");


        Dataset<Row> dataset = spark.read()
//                .format("com.crealytics.spark.excel")
                .format("excel")
                .options(options)
                .load("data/giantFile.xlsx");


        System.out.println("Dataframe's schema:");
        System.out.println(dataset.count());
        dataset.printSchema();
        dataset.show();
    }
}
