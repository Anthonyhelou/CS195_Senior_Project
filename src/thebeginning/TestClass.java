package thebeginning;
//imports
import java.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;
//to disable redundant logs
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class TestClass {

    private String testString;
    private HashMap<Integer, String> javaHashMap; //Integer not int, need generic
    private ArrayList<String> javaArrayList;
    private Stack<Integer> javaStack;
    private Queue<Integer> javaQueue;


    //for shits and shiggles
    public void TestClass () {
        this.testNumber = 10000000;
        this.testString = "Concurrency";
        this.javaHashMap = new HashMap<>();
        this.javaArrayList = new ArrayList<>();
        this.javaStack = new Stack<>();
        this.javaQueue = new LinkedList<>();    //java Queue FIFO implemented as LL
    }


    public void setTestNumber(int testNumber) {
        this.testNumber = testNumber;
    }

    public void setTestString(String testString) {
        this.testString = testString;
    }

    public void setJavaHashMap(HashMap<Integer, String> javaHashMap) {
        this.javaHashMap = javaHashMap;
    }

    public void setJavaArrayList(ArrayList<String> javaArrayList) {
        this.javaArrayList = javaArrayList;
    }

    public void setJavaStack(Stack<Integer> javaStack) {
        this.javaStack = javaStack;
    }

    public void setJavaQueue(Queue<Integer> javaQueue) {
        this.javaQueue = javaQueue;
    }

    //shits and shiggles
    private int testNumber;

    public int getTestNumber() {
        return testNumber;
    }

    public String getTestString() {
        return testString;
    }

    public HashMap<Integer, String> getJavaHashMap() {
        return javaHashMap;
    }

    public ArrayList<String> getJavaArrayList() {
        return javaArrayList;
    }

    public Stack<Integer> getJavaStack() {
        return javaStack;
    }

    public Queue<Integer> getJavaQueue() {
        return javaQueue;
    }





    public static void main (String [] args) {

        System.out.println("Will demo a simple program that parses a 10 megabyte file" +
                "in less than .1 seconds using apache Spark");

        //uncomment this if you want a ton of log errors to display in the console
        Logger.getLogger("org").setLevel(Level.ERROR);
        //so we can have our hadoop path set up. Download hadoop and set this path if needed
        System.setProperty("hadoop.home.dir", "C:\\winutil");


        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile = sc.textFile("src/resources/shakespeare.txt");
        //do not think about how this code works its a waste of time
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));





    }



}
