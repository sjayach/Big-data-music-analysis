package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

/**
 * A mock source to provide data
 */
public class MockSource implements Source<Status> {
    int index = 0;

    @Override
    public boolean hasNext() {
        return index < 1;
    }

    @Override
    public Collection<Status> next() {
    	List<Status> status=new ArrayList<Status>();
    	try{
    		JSONObject mockStatus=new JSONObject();
    		mockStatus.put("text", "#NowPlaying Lemon Tree by Fools Garden ? https://t.co/9ObZSfrdWs");
    		mockStatus.put("created at",new Date());
    	Status status1=DataObjectFactory.createStatus(mockStatus.toString());
    	status.add(status1);
    	JSONObject mockStatus2=new JSONObject();
		mockStatus2.put("text", "#NowPlaying Better Place by Rachel Platten from the album Wildfire - iTunes: https://t.co/klA6GpnFl2");
		mockStatus2.put("created at",new Date());
    	Status status2=DataObjectFactory.createStatus(mockStatus2.toString());
    	status.add(status2);
    	JSONObject mockStatus3=new JSONObject();
		mockStatus3.put("text", "null");
		mockStatus3.put("created at",new Date());
    	Status status3=DataObjectFactory.createStatus(mockStatus3.toString());
    	status.add(status3);
    	
    	JSONObject mockStatus4=new JSONObject();
		mockStatus4.put("text", "This is not related to musix, Hence we re not expecting this in the list");
		mockStatus4.put("created at",new Date());
    	Status status4=DataObjectFactory.createStatus(mockStatus4.toString());
    	status.add(status4);
    	}catch(TwitterException | JSONException e){
    		e.printStackTrace();
    	}
    	return status;
    }
}
