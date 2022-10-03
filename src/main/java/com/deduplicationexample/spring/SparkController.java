package com.deduplicationexample.spring;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.ServletContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.parquet.example.Paper.schema;

//@RequestMapping("/spark-context")
@RestController
public class SparkController {
    private final SparkSession sparkSession;
    final
    ServletContext context;
    public SparkController(SparkSession sparkSession, ServletContext context) {
        this.sparkSession = sparkSession;
        this.context = context;
    }

    @RequestMapping(value = "/read-csv", method = RequestMethod.POST)
    public HashMap<String,String> getRowCount(@RequestParam("csv_url") String csv_url,@RequestParam("remove_duplicate_column") String remove_duplicate_column) {

        String downloaded_filename = getAlphaNumericString(10) + ".csv";
        String complete_file_path = System.getProperty("user.dir") + "/savedcsvs" + "/" + downloaded_filename;
        fileDownloadUsingFilesCopy(csv_url, complete_file_path);
        Dataset<Row> dataset = this.sparkSession.read().option("header", "true").csv(complete_file_path);

        Dataset<Row> dataset_duplicates_removed = dataset.dropDuplicates(remove_duplicate_column);

        Dataset<Row> dataset_discarded = dataset.union(dataset_duplicates_removed).except(dataset.intersect(dataset_duplicates_removed));
        //Dataset<Row> dataset_discarded = dataset.union(dataset_duplicates_removed).except(dataset.intersect(dataset_duplicates_removed));

        final String baseUrl = ServletUriComponentsBuilder.fromCurrentContextPath().build().toUriString();

        // upload filtered file to folder
        String dataset_duplicates_removed_filename = getAlphaNumericString(10) + ".csv";

        dataset_duplicates_removed.coalesce(1)
                .write()
                .option("header","true")
                .option("sep",",")
                .mode("overwrite")
                .csv(System.getProperty("user.dir") + "/savedcsvs" + "/" +  dataset_duplicates_removed_filename);

        // upload filtered file to folder
        String dataset_discarded_filename = getAlphaNumericString(10) + ".csv";

        dataset_discarded.coalesce(1)
                .write()
                .option("header","true")
                .option("sep",",")
                .mode("overwrite")
                .csv(System.getProperty("user.dir") + "/savedcsvs" + "/" +  dataset_discarded_filename);

        HashMap<String, String> map = new HashMap<>();
        map.put("duplicate_removed_file_url", baseUrl + "/savedcsvs/" + downloaded_filename);
        map.put("discarded_file_url", baseUrl + "/savedcsvs/" + dataset_discarded_filename);
        return map;
    }

    // Method-4: File Download using Files.copy()
    private static void fileDownloadUsingFilesCopy(String crunchifyURL, String crunchifyFileLocalPath) {
        try (InputStream crunchifyInputStream = URI.create(crunchifyURL).toURL().openStream()) {
            Files.copy(crunchifyInputStream, Paths.get(crunchifyFileLocalPath), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getAlphaNumericString(int n)
    {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

    @GetMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }

}
