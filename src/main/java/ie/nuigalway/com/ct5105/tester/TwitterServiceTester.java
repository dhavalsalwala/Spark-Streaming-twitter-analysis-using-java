package ie.nuigalway.com.ct5105.tester;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import java.util.List;

import ie.nuigalway.com.ct5105.service.TwitterService;
import ie.nuigalway.com.ct5105.utils.CommonUtils;
import twitter4j.Status;

/**
 * Twitter Service Tester
 *
 * @author Dhaval Salwala 18230845
 *
 *         Note: Please change the checkpoint directory as per your
 *         OS/Configuration. Line no:90
 *
 */
public class TwitterServiceTester
{
    static
    {
	// switching off spark logs for clear output.
	Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
	Logger.getLogger("org.apache.spark.storage.BlockManager")
		.setLevel(Level.ERROR);
    }

    public static void main(String[] args) throws Exception
    {
	final SparkConf sparkConf = new SparkConf();
	sparkConf.setAppName("Assignment 5");
	sparkConf.setMaster("local[4]");

	CommonUtils.configureTwitterCredentials();

	final JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
		new Duration(1000));
	final JavaDStream<Status> tweets = TwitterUtils.createStream(ssc);
	final TwitterService service = new TwitterService();

	// Q1 Prints a sample (10 messages) of the tweets it receives from
	// Twitter every second.
	final JavaDStream<String> messages = service.getMessages(tweets);
	messages.print();

	// Q2 Prints total number of words in each tweet.
	// Example: (Tweet: <Message>,Total Words: <no of words>)
	final JavaPairDStream<String, String> words = service
		.getTotalWords(messages);
	words.print();

	// Q2 Prints total number of characters in each tweet.
	// Example: (Tweet: <Message>,Total Characters: <no of characters>)
	final JavaPairDStream<String, String> characters = service
		.getTotalCharacters(messages);
	characters.print();

	// Q2 Prints total number of hashtags in each tweet.
	// Example: (Tweet: <message> Hastags: ,[])
	final JavaPairDStream<String, List<String>> hashtags = service
		.getHashtags(messages);
	hashtags.print();

	// Q3(a) Prints average number of words per tweet.
	service.findAvgOfWords(messages);

	// Q3(a) Prints average number of characters per tweet.
	service.findAvgOfCharacters(messages);

	// Q3(b) Prints top 10 hashtags from a batch.
	final int noOfHastags = 10;
	service.getTopNHashtags(hashtags, noOfHastags);

	// Q3(c) Repeat computation for the last 5 minutes of tweets every
	// 30sec.
	final int interval = 30 * 1000;
	final int windowSize = 5 * 60 * 1000;
	service.performWindowedOperations(messages, noOfHastags, windowSize,
		interval);

	// initialising checkpoint
	ssc.checkpoint("/tmp/"); // change as per your OS / Configuration

	// starting java streaming context
	ssc.start();
	ssc.awaitTermination();
    }
}
