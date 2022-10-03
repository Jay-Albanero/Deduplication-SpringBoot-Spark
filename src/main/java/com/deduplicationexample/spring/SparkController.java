package com.deduplicationexample.spring;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

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

    //@GetMapping("")
    @RequestMapping(value = "/read-csv", method = RequestMethod.POST)
    public ResponseEntity<String> getRowCount(@RequestParam("csv_url") String csv_url,@RequestParam("remove_duplicate_column") String remove_duplicate_column) {

        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String downloaded_filename = new String(array, Charset.forName("UTF-8")) + ".csv";

        //String absolutePath = this.context.getRealPath("savedcsvs") + "/" + downloadedfilename;
        Path absolutePath = Paths.get(this.context.getRealPath("savedcsvs")) ;
        String absolutePathFile = absolutePath + "/" + downloaded_filename;

        crunchifyFileDownloadUsingFilesCopy(csv_url, System.getProperty("user.dir") + "/savedcsvs" + "/" + downloaded_filename);

        URL url = getClass().getClassLoader().getResource("raw_data.csv");
        Dataset<Row> dataset = this.sparkSession.read().option("header", "true").csv(url.getPath());

        Dataset<Row> dataset_new = dataset.dropDuplicates("Patient Number");

        URL url2 = getClass().getClassLoader().getResource("raw_data2.csv");

        //dataset_new.write().format("csv").save(url2.getPath());
//        dataset_new.coalesce(1)
//                .write()
//                .option("header","true")
//                .option("sep",",")
//                .mode("overwrite")
//                .csv("/home/jay/IdeaProjects/Duplication-SpringBoot-Spark/savedcsvs");

        //Dataset<Row> dataset_new1 = dataset.unionAll(dataset_new).except(dataset.intersect(dataset_new));
        Dataset<Row> dataset_new1 = dataset.union(dataset_new).except(dataset.intersect(dataset_new));
        //Dataset<Row> dataset_new1 = dataset.union(dataset_new).except(dataset.intersect(dataset_new));

//        Dataset<Row> dataset_new1 = dataset.map(row -> {
//                row.getString();
//        });



        String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                //String.format("<h2>%s</h2>", "Spark version = "+sparkSession.sparkContext().version()) +
                String.format("<h3>%s</h3>", "Read csv..") +
                String.format("<h4>Total records %d</h4>", dataset.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString()) +
                dataset.showString(20, 20, true)

                +

                String.format("<br><br><br><br><br><br><br><h4>Total records after duplication removed %d</h4>", dataset_new.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data Duplication Removed - <br/>", dataset_new.schema().treeString()) +
                dataset_new.showString(20, 20, true)

                +

                String.format("<br><br><br><br><br><br><br><h4>Total records duplicates %d</h4>", dataset_new1.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> Dupliates - <br/>", dataset_new1.schema().treeString()) +
                dataset_new1.showString(20, 20, true)

//                +
//
//                String.format("<br><br><br><br><br><br><br>\\n\\n\\n\\n<h4>Url %d</h4>", url2.getPath())

                ;

//        HashMap<String, String> map = new HashMap<>();
//        map.put("Message", html);
//        map.put("csv1", url.getPath());
//        map.put("csv2", "bb");
//        return map;

        return ResponseEntity.ok(html);
    }

    // Method-4: File Download using Files.copy()
    private static void crunchifyFileDownloadUsingFilesCopy(String crunchifyURL, String crunchifyFileLocalPath) {

        // URI Creates a URI by parsing the given string.
        // This convenience factory method works as if by invoking the URI(String) constructor;
        // any URISyntaxException thrown by the constructor is caught and wrapped in a new IllegalArgumentException object, which is then thrown.
        try (InputStream crunchifyInputStream = URI.create(crunchifyURL).toURL().openStream()) {

            // Files: This class consists exclusively of static methods that operate on files, directories, or other types of files.
            // copy() Copies all bytes from an input stream to a file. On return, the input stream will be at end of stream.

            // Paths: This class consists exclusively of static methods that return a Path by converting a path string or URI.
            // Paths.get() Converts a path string, or a sequence of strings that when joined form a path string, to a Path.
            Files.copy(crunchifyInputStream, Paths.get(crunchifyFileLocalPath), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // printStackTrace: Prints this throwable and its backtrace to the standard error stream.
            e.printStackTrace();
        }

        //printResult("File Downloaded Successfully with Files.copy() Operation \n");
    }

    private static void printResult(String s) {
    }

    @GetMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }

}
