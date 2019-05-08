package ie.nuigalway.com.ct5105.utils;

import java.util.Properties;

/**
 * Twitter credential configuration utility.
 *
 * @author Dhaval Salwala 18230845
 *
 */
public class CommonUtils
{
    public static void configureTwitterCredentials() throws Exception
    {
	final Properties prop = new Properties();
	prop.load(CommonUtils.class.getClassLoader()
		.getResourceAsStream("twitter.properties"));

	System.setProperty("twitter4j.oauth.consumerKey",
		prop.getProperty("consumerKey"));
	System.setProperty("twitter4j.oauth.consumerSecret",
		prop.getProperty("consumerSecret"));
	System.setProperty("twitter4j.oauth.accessToken",
		prop.getProperty("accessToken"));
	System.setProperty("twitter4j.oauth.accessTokenSecret",
		prop.getProperty("accessTokenSecret"));
    }
}
