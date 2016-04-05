import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


//The graph is stored as a group of nodes. And each node has connections with other nodes
class Node {
	
	String hashtagName;
	HashMap<String, Node> connections;
	
	Node(String hashtagName){
		this.hashtagName = hashtagName;
		connections = new HashMap<String, Node>();
	}
}

class Graph {
	HashMap<String,Node> nodes;
	
	Graph() {
		nodes = new HashMap<String, Node>();
	}

}

//this tweet object stores the list of hashtags in the tweet. And it stores the time of the tweet in the form of Date object
class Tweet {
	ArrayList<String> hashtags;
	Date time;
	
	Tweet(Date time, ArrayList<String> hashtags){
		this.hashtags = hashtags;
		this.time = time;
	}
}

//A custom comparator for the treeset data structure used in the project
class Mycomparator implements Comparator<Tweet>{
	public int compare(Tweet t1, Tweet t2){
		if(average_degree.findTimeDiff(t1.time, t2.time)>=0){
			return 1;
		}else{
			return -1;
		}
	}
}

public class average_degree {
	public static void main(String args[]){
		//the function that takes input of tweets and computes the degrees and writes in the output file
		findDegrees();
	}
	
	public static void findDegrees(){
		String outputFileName = "tweet_output/output.txt";
		BufferedReader br = null;
		BufferedWriter bufferedWriter = null;
		FileWriter fileWriter;
		try {
			String jsonData = "";
			String line;
			br = new BufferedReader(new FileReader("tweet_input/tweets.txt")); 
            fileWriter = new FileWriter(outputFileName);
            bufferedWriter = new BufferedWriter(fileWriter);
            //a treeset data structure used to store the tweets in order of time
			TreeSet<Tweet> currentTweets = new TreeSet<Tweet>(new Mycomparator());
			//a hashmap used to store all the current(current means in 60 second window) hashtags in the graph as keys and the 
			//node corresponding to each hashtag as value
			HashMap<String, Node> currentHashTags = new HashMap<String, Node>();
			//HashMap used to store all the current edges in graph and the value stores the count of edges though only one 
			//edge is added in the graph
			HashMap<String, Integer> currentEdges = new HashMap<String, Integer>();
			//this the graph data structure i am using in this project
			Graph graph = new Graph();
			//each line from the test is a tweet and is retrieved
			while((line = br.readLine()) != null){
				jsonData = line;
				JSONObject obj = new JSONObject(jsonData);
				if(obj.has("limit")) {
					continue;
				}
				String created_at = obj.getString("created_at");
				String[] timeArr = created_at.split(" ");
				timeArr[4] = timeArr[4].replace("+", "");
				Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(timeArr[1]);
			    Calendar cal = Calendar.getInstance();
			    cal.setTime(date);
			    int month = cal.get(Calendar.MONTH);
			    String time ="";
			    if(month<10){
			    	time = timeArr[5]+"/0"+month+"/"+timeArr[2]+" "+timeArr[3]+":"+timeArr[4];
			    }else{
			    	time = timeArr[5]+"/"+month+"/"+timeArr[2]+" "+timeArr[3]+":"+timeArr[4];
			    }
			    SimpleDateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss:SSSS");

			    Date d1 = null;
			    d1 = format.parse(time);

				JSONObject entities = obj.getJSONObject("entities");
				JSONArray hashtagsArray = entities.getJSONArray("hashtags");
				ArrayList<String> hashtags = new ArrayList<String>();
				for(int i=0; i<hashtagsArray.length(); i++){
					JSONObject hashtagObject = hashtagsArray.getJSONObject(i);
					if(!hashtags.contains(hashtagObject.getString("text"))){
						hashtags.add(hashtagObject.getString("text"));
					}
				}
				//A tweet object is formed after retrieving the time and hashtags of json data
				Tweet tweet = new Tweet(d1, hashtags);
				//if there are no current tweets, then the new tweet(the tweet just retieved from json) is added into the 
				//currentTweets treeset and the hashtags are added to currentHashTags and edges are added to currentEdges. 
				//The below if condition runs if there are current tweets
				if(currentTweets.size() != 0){
					//findTimeDiff finds difference in times of two tweets
					if(findTimeDiff(tweet.time, currentTweets.last().time)<0){
						//the tweet is out of order. If the diff in time between 'out of order tweet' and the latest tweets
						//is less than 60, then the 'out of order tweet' should be added to the graph
						if(findTimeDiff(currentTweets.last().time, tweet.time)<60){
							currentTweets.add(tweet);
						}else{
							//the current tweet should not be added to the graph
							computeDegree(graph, bufferedWriter);
							continue;
						}
					}else{
						//the tweet is not out of order
						Iterator itr = currentTweets.iterator();
						Tweet tempTweet;
						//the below while loop is to evict the older tweets
						while(itr.hasNext()&&(findTimeDiff(tweet.time, (tempTweet=(Tweet)itr.next()).time)>=60)){
							ArrayList<String> hashTags = tempTweet.hashtags;
							int forCount = 0;
							//if the tweet of the iterator has less than 2 tags, then that means it has never been added to
							//the graph, so it should just be removed from the current tweets
							if(hashTags.size()<2) {
								itr.remove();
								continue;
							}
							for(int j=0; j<hashTags.size(); j++){
								Node fromNode = currentHashTags.get(hashTags.get(j));
								for(int k=j+1; k<hashTags.size(); k++){
									String forward = hashTags.get(j)+hashTags.get(k);
									String reverse = hashTags.get(k)+hashTags.get(j);
									boolean removeEdge = false;
									//if the count of edge is more than one (that means the edge is repeated in other tweets of current tweets),
									//then just decrement count, else remove the edge
									if(currentEdges.containsKey(forward)){
										int count = currentEdges.get(forward);
										count--;
										if(count == 0){
											//remove the edge from current edges
											currentEdges.remove(forward);
											removeEdge = true;
										}
									}else if(currentEdges.containsKey(reverse)){
										int count = currentEdges.get(reverse);
										count--;
										if(count == 0){
											//remove the edge from current edges
											currentEdges.remove(reverse);
											removeEdge = true;
										}
									}else{
										//this part should not be reached
									}
									if(removeEdge == true){
										//remove the edges from the graph
										Node toNode = currentHashTags.get(hashTags.get(k));
										fromNode.connections.remove(hashTags.get(k));
										toNode.connections.remove(hashTags.get(j));
									}
								}
				
								if(fromNode.connections.size()==0) {
									//remove the node from the graph, if it has no connections
									graph.nodes.remove(hashTags.get(j));
									currentHashTags.remove(hashTags.get(j));
								}
								
							}
							//remove the tweet being iterated
							itr.remove();
							
						}
						currentTweets.add(tweet);
					}
				}else {
					//add the new tweet
					currentTweets.add(tweet);
				}
					//check if the hashtags of the tweets already exist in the graph. Create new nodes
					//for the new hashtags
					ArrayList<String> hashTags = tweet.hashtags;
					//Dont add if it to the graph is the only hashtag in the tweet
					if(hashTags.size()>1) {
						Node tempNode = null;
						for(int j = 0; j< hashTags.size(); j++){
							if(currentHashTags.get(hashTags.get(j)) != null){
								
							}else{
								
								tempNode = new Node(hashTags.get(j));
								//store the new nodes in currentHadhTags and graph
								currentHashTags.put(hashTags.get(j), tempNode);
								graph.nodes.put(hashTags.get(j),tempNode);
							}
						}
						
						for(int j=0; j<hashTags.size(); j++){ 
							Node fromNode = currentHashTags.get(hashTags.get(j));
							for(int k=j+1; k<hashTags.size(); k++){
								//add the new edges formed by new tweet to currentedges
								String forward = hashTags.get(j)+hashTags.get(k);
								String reverse = hashTags.get(k)+hashTags.get(j);
								
								if(currentEdges.containsKey(forward)){
									int count = (Integer)currentEdges.get(forward);
									count++;
									currentEdges.put(forward, count);
								}else if(currentEdges.containsKey(reverse)){
									int count = (Integer)currentEdges.get(reverse);
									count++;
									currentEdges.put(reverse, count);
								} else {
									currentEdges.put(forward, 1);
								}
								Node toNode = currentHashTags.get(hashTags.get(k));
								//add the new edges to the grpah, if they don't exist before
								if(!fromNode.connections.containsKey(hashTags.get(k))){
									fromNode.connections.put(hashTags.get(k), toNode);
								}
					
								if(!toNode.connections.containsKey(hashTags.get(j))){
									toNode.connections.put(hashTags.get(j), fromNode);
								}
							}
						}
					}
					//calculate the average degree of the graph
					computeDegree(graph, bufferedWriter);
			}
			
		}catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
					bufferedWriter.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		try {
			br.close();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void computeDegree(Graph graph, BufferedWriter bw){
		DecimalFormat d2 = new DecimalFormat("###.##");
		d2.setRoundingMode(RoundingMode.DOWN);
		int nodesCount = 0;
		double averageDegree = 0;
		for(Map.Entry entry : graph.nodes.entrySet()){
			nodesCount++;
			HashMap<String, Node> edges = ((Node)entry.getValue()).connections;
			averageDegree += edges.size();
		}
		if(nodesCount != 0) averageDegree /= (double)nodesCount;
		//truncating the average degree to two decimals
		BigDecimal truncatedDegreeCount = new BigDecimal(String.valueOf(averageDegree)).setScale(2, BigDecimal.ROUND_FLOOR);
		try {
			bw.write(String.valueOf(truncatedDegreeCount));
			bw.write("\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//function to find time difference between the times of two tweets
	public static long findTimeDiff(Date d1, Date d2){
		long diff = d1.getTime() - d2.getTime();
	    long diffSeconds = diff / 1000;
	    return diffSeconds;
	}
}

