package ie.nuigalway.com.ct5105.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;
import twitter4j.Status;

/**
 * Perform various operations on twitter stream.
 *
 * @author Dhaval Salwala 18230845
 *
 */
public class TwitterService implements Serializable
{

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Q1 Prints a sample (10 messages) of the tweets it receives from Twitter
     * every second.
     *
     * @param tweets
     *            Stream of tweets from the streaming context.
     * @return
     */
    public JavaDStream<String> getMessages(JavaDStream<Status> tweets)
    {
	return tweets.map(Status::getText);
    }

    /**
     * Prints total number of words in each tweet.
     *
     * @param messages
     *            tweets in the DStream format.
     * @return
     */
    public JavaPairDStream<String, String> getTotalWords(
	    JavaDStream<String> messages)
    {
	return messages.mapToPair(t -> new Tuple2<String, String>("Tweet: " + t,
		" Total Words: " + Arrays.asList(t.split(" ")).size()));
    }

    /**
     * Q2 Prints total number of characters in each tweet.
     *
     * @param messages
     *            tweets in the DStream format.
     * @return
     */
    public JavaPairDStream<String, String> getTotalCharacters(
	    JavaDStream<String> messages)
    {
	return messages.mapToPair((String t) -> new Tuple2<String, String>(
		"Tweet: " + t,
		" Total Characters: " + Arrays.asList(t.split("")).size()));
    }

    /**
     * Prints total number of hashtags in each tweet.
     *
     * @param messages
     *            tweets in the DStream format.
     * @return
     */
    public JavaPairDStream<String, List<String>> getHashtags(
	    JavaDStream<String> messages)
    {
	return messages
		.mapToPair((String t) -> new Tuple2<String, List<String>>(
			"Tweet: " + t + " Hastags: ",
			Arrays.asList(t.split(" ")).stream()
				.filter(x -> x.startsWith("#"))
				.collect(Collectors.toList())));
    }

    long noOfWords = 0L;
    long noOfCharacters = 0L;
    long noOfTweets = 0L;

    /**
     * Prints average number of words per tweet.
     *
     * @param messages
     *            tweets in the DStream format.
     */
    public void findAvgOfWords(JavaDStream<String> messages)
    {

	messages.foreachRDD(x -> {
	    noOfTweets += x.count();
	});

	messages.flatMap(t -> Arrays.asList(t.split(" ")).iterator())
		.foreachRDD(w -> {
		    noOfWords += w.count();
		    if (noOfTweets > 0L)
		    {
			System.out.println();
			System.out.println("Total Words: " + noOfWords);
			System.out.println("Total Tweets: " + noOfTweets);
			System.out.println(
				"Average number	of words per tweet : "
					+ (Double.valueOf(noOfWords)
						/ Double.valueOf(noOfTweets)));

			noOfWords = 0L;
			noOfTweets = 0L;
		    }
		});

    }

    /**
     * Prints average number of characters per tweet.
     *
     * @param messages
     *            tweets in the DStream format.
     */
    public void findAvgOfCharacters(JavaDStream<String> messages)
    {
	messages.foreachRDD(x -> {
	    noOfTweets += x.count();
	});

	messages.flatMap(t -> Arrays.asList(t.split("")).iterator())
		.foreachRDD(w -> {
		    noOfCharacters += w.count();
		    if (noOfCharacters > 0L)
		    {
			System.out.println();
			System.out
				.println("Total Characters: " + noOfCharacters);
			System.out.println("Total Tweets: " + noOfTweets);
			System.out.println(
				"Average number	of characters per tweet : "
					+ (Double.valueOf(noOfCharacters)
						/ Double.valueOf(noOfTweets)));

			noOfCharacters = 0L;
			noOfTweets = 0L;
		    }
		});
    }

    /**
     * Prints top 10 hashtags from a batch.
     *
     * @param hashTags
     *            Tuple of <Tweet, List of Hashtags> in the DStream format.
     * @param n
     *            no of hashtags to display.
     */
    public void getTopNHashtags(JavaPairDStream<String, List<String>> hashTags,
	    int n)
    {
	final JavaPairDStream<Integer, String> sortedHastags = hashTags
		.map(tuple -> tuple._2()).flatMap(hastag -> hastag.iterator())
		.mapToPair(hastag -> new Tuple2<String, Integer>(hastag, 1))
		.reduceByKey((x, y) -> x + y).mapToPair(tuple -> tuple.swap())
		.transformToPair(tuple -> tuple.sortByKey(false));

	sortedHastags
		.foreachRDD(new VoidFunction<JavaPairRDD<Integer, String>>()
		{
		    private static final long serialVersionUID = 1L;

		    @Override
		    public void call(JavaPairRDD<Integer, String> rdd)
		    {
			String out = "\nTop 10 hashtags:\n";
			for (final Tuple2<Integer, String> t : rdd.take(n))
			{
			    out = out + t.toString() + "\n";
			}
			System.out.println(out);
		    }
		});
    }

    /**
     * Repeat computation for the window of length windowSize.
     *
     * @param messages
     *            tweets in the DStream format.
     * @param n
     *            no of hashtags to display.
     * @param windowSize
     *            window size for displaying total hashtags
     * @param interval
     *            time interval for displaying hashtags
     */
    public void performWindowedOperations(JavaDStream<String> messages, int n,
	    long windowSize, long interval)
    {
	final JavaDStream<String> windowedMessages = messages
		.window(new Duration(windowSize), new Duration(interval));

	findAvgOfWords(windowedMessages);
	findAvgOfCharacters(windowedMessages);
	getTopNHashtags(getHashtags(windowedMessages), n);
    }
}
