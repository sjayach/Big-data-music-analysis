package edu.csula.datascience.acquisition;



import java.io.IOException;
import java.net.URISyntaxException;
public class TwitterCollectorApp {
	
    public static void main(String[] args) throws IOException, URISyntaxException {
    	
    	
    	TwitterSource2 source = new TwitterSource2("#NowPlaying");
    	source.readTwitterFeed();
      }
}
