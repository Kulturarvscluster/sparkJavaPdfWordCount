package dk.kb;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.input.PortableDataStream;
import org.jwat.arc.ArcRecord;
import org.jwat.archive.ArchiveParser;
import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.common.UriProfile;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;

import java.awt.*;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WarcCookieReader {
    
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 1) {
            System.err.println("Usage: WarcCookieReader <path>");
            System.exit(1);
        }
        
        
        SparkConf sparkConf = new SparkConf().setAppName("WarcCookieReader");
        
        //Configure how many resources we need from the cluster
        configureResourceAllocation(sparkConf);
    
    
        //Of of the many ways to reduce spark logging
        //LogManager.getLogger("org").setLevel(Level.WARN);
        
        try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {
    
    
            //Read all files in dir
            JavaPairRDD<String, PortableDataStream> pdfFiles = ctx.binaryFiles(args[0]);
    
            
            //Extract records from warc files
            JavaRDD<WarcRecord> records = getWarcRecordJavaRDD(pdfFiles);
    
    
            JavaPairRDD<String, HttpHeader> headers = getHttpHeaders(records);
    
            JavaPairRDD<String, String> cookies = getCookieHeaders(headers);
    
    
            Map<String, String> collectedCookies = cookies.collectAsMap();
            for (Map.Entry<String, String> stringStringEntry : collectedCookies.entrySet()) {
                System.out.println(stringStringEntry.getKey() + " -> " + stringStringEntry.getValue()) ;
            }
        }
    }
    
    private static JavaPairRDD<String, String> getCookieHeaders(JavaPairRDD<String, HttpHeader> headers) {
        return headers.flatMapToPair(
                        (Tuple2<String, HttpHeader> v1) -> v1._2()
                                                             .getHeaderList()
                                                             .stream()
                                                             .filter(headerLine -> headerLine.name.equals("Set-Cookie"))
                                                             .map(headerLine -> headerLine.value)
                                                             .map(s -> new Tuple2<>(v1._1, s))
                                                             .iterator());
    }
    
    private static JavaPairRDD<String, HttpHeader> getHttpHeaders(JavaRDD<WarcRecord> records) {
        return records.flatMapToPair(
                        (PairFlatMapFunction<WarcRecord, String, HttpHeader>)
                                warcRecord -> warcRecord
                                                      .getHeaderList()
                                                      .stream()
                                                      .filter(headerLine ->
                                                                      headerLine.name.equals("WARC-Target-URI"))
                                                      .filter(headerLine -> warcRecord.getHttpHeader() != null)
                                                      .map(headerLine -> new Tuple2<>(
                                                              headerLine.value,
                                                              warcRecord.getHttpHeader()
                                                      ))
                                                      .iterator()
                );
    }
    
    private static JavaRDD<WarcRecord> getWarcRecordJavaRDD(JavaPairRDD<String, PortableDataStream> pdfFiles) {
        return pdfFiles.flatMap(
                        (Tuple2<String, PortableDataStream> warcFile) -> {
                            WarcReader reader = WarcReaderFactory.getReader(warcFile._2().open());
                            return new Iterator<WarcRecord>() {
                                private WarcRecord next = null;
    
                                @Override
                                public boolean hasNext() {
                                    if (next == null) {
                                        try {
                                            next = reader.getNextRecord();
                                        } catch (IOException e) {
                                            throw new UncheckedIOException(e);
                                        }
                                    }
                                    boolean hasNext = (next != null);
                                    if (!hasNext){
                                        reader.close();
                                    }
                                    return hasNext;
                                }
    
                                @Override
                                public WarcRecord next() {
                                    if (!hasNext()) {
                                        throw new NoSuchElementException();
                                    }
                                    //Return the next and set next to null, so hasNext will read it later
                                    WarcRecord result = next;
                                    next = null;
                                    return result;
                                }
                            };
        
                        });
    }
    
    /**
     * Set Spark configuration
     * This can also be done from on the command line and through a properties file.
     * @param sparkConf The spark configuration object to configure
     * @return the configured sparkConf object, in case you need it...
     */
    protected static SparkConf configureResourceAllocation(SparkConf sparkConf) {
        //These apparently only function when we use master=yarn property
        //These properties can be set in a config file or as command line params,
        // or in the code, as seen here
        
        //Hver executor skal have 25GB RAM (default setting for KAC)
        // Set ned til 2G hvis du skal køre test
        //sparkConf.set("spark.executor.memory", "2G");
        
        //Og hver executor skal bruge 4 kerner (default).
        //Sæt ned til 1 hvis du skal køre test.
        //sparkConf.set("spark.executor.cores", "1");
        
        //Vi kan max have 26 executors
        sparkConf.set("spark.dynamicAllocation.maxExecutors", "26");
        //Og min 1 executor
        sparkConf.set("spark.dynamicAllocation.minExecutors", "0");
        //Og vi starter med 1
        sparkConf.set("spark.dynamicAllocation.initialExecutors", "3");
        
        return sparkConf;
    }
    
}
