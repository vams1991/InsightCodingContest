import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

class Node {
	
	String hashtagName;
	HashMap<String, Node> connections;
	
	Node(String hashtagName){
		this.hashtagName = hashtagName;
		connections = new HashMap<String, Node>();
	}
	void print(){
		System.out.println("---------------");
		System.out.println("Node Name: "+hashtagName);
		for(Map.Entry entry : connections.entrySet()){
			System.out.print(entry.getKey()+" ,");
		}
		System.out.println();
		System.out.println("---------------");
	}
}

class Graph {
	HashMap<String,Node> nodes;
	
	Graph() {
		nodes = new HashMap<String, Node>();
	}
	
	void print(){
		System.out.println("Graph");
		System.out.println("-------------");
		for(Map.Entry entry : nodes.entrySet()){
			System.out.print(entry.getKey()+":");
			Node node = (Node) entry.getValue();
			HashMap<String, Node> map = node.connections;
			for(Map.Entry entry1 : map.entrySet()){
				System.out.print(entry1.getKey()+" ,");
			}
			System.out.println();
		}
		System.out.println("-------------");
	}
}

class Tweet {
	ArrayList<String> hashtags;
	Date time;
	
	Tweet(Date time, ArrayList<String> hashtags){
		this.hashtags = hashtags;
		this.time = time;
	}
}

class Mycomparator implements Comparator<Tweet>{
	public int compare(Tweet t1, Tweet t2){
		return (int)average_degree.findTimeDiff(t1.time, t2.time);
	}
}

public class average_degree {
	public static void main(String args[]){
		
		ArrayList<Tweet> tweets = new ArrayList<Tweet>();
		//ArrayList<String> htags1 = new ArrayList<String>();
		//htags1.add("Spark");
		//htags1.add("Apache");
		//tweets.add(new Tweet(10, htags1));
		//ArrayList<String> htags2 = new ArrayList<String>();
		//htags2.add("Apache");
		//htags2.add("Hadoop");
		//htags2.add("Storm");
		//tweets.add(new Tweet(15, htags2));
		//ArrayList<String> htags3 = new ArrayList<String>();
		//htags3.add("Apache");
		//tweets.add(new Tweet(30, htags3));
		//ArrayList<String> htags4 = new ArrayList<String>();
		//htags4.add("Flink");
		//htags4.add("Spark");
		//tweets.add(new Tweet(55, htags4));
		//ArrayList<String> htags5 = new ArrayList<String>();
		//htags5.add("Spark");
		//htags5.add("HBase");
		//tweets.add(new Tweet(58, htags5));
		//ArrayList<String> htags6 = new ArrayList<String>();
		//htags6.add("Hadoop");
		//htags6.add("Apache");
		//tweets.add(new Tweet(72, htags6));
		//ArrayList<String> htags7 = new ArrayList<String>();
		//htags7.add("Flink");
		//htags7.add("HBase");
		//tweets.add(new Tweet(70, htags7));
		//ArrayList<String> htags8 = new ArrayList<String>();
		//htags8.add("Cassandra");
		//htags8.add("NoSQL");
		//tweets.add(new Tweet(10, htags8));
		//ArrayList<String> htags9 = new ArrayList<String>();
		//htags9.add("Kafka");
		//htags9.add("Apache");
		//tweets.add(new Tweet(80, htags9));
		System.out.println(findDegrees());
	}
	
	public static ArrayList<Double> findDegrees(){
		String outputFileName = "tweet_input/output.txt";
		BufferedReader br = null;
		BufferedWriter bufferedWriter = null;
		FileWriter fileWriter;
		ArrayList<Double> degrees = new ArrayList<Double>();
		try {
			String jsonData = "";
			
			String line;
			br = new BufferedReader(new FileReader("tweet_output/tweets.txt")); 
            		fileWriter = new FileWriter(outputFileName);
            		bufferedWriter = new BufferedWriter(fileWriter);
			TreeSet<Tweet> currentTweets = new TreeSet<Tweet>(new Mycomparator());
			HashMap<String, Node> currentHashTags = new HashMap<String, Node>();
			HashMap<String, Integer> currentEdges = new HashMap<String, Integer>();
			Graph graph = new Graph();
			int loopCount=0;
			int limitCount = 0;
			while((line = br.readLine()) != null){
				loopCount++;
				System.out.println("loopCount is: "+loopCount);
				System.out.println("limitCount: "+limitCount);
				jsonData = line;
				JSONObject obj = new JSONObject(jsonData);
				if(obj.has("limit")) {
					limitCount++;
					continue;
				}
				String created_at = obj.getString("created_at");
				System.out.println(created_at);
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
			    //System.out.println(time);
			    SimpleDateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss:SSSS");

			    Date d1 = null;
			    d1 = format.parse(time);

				JSONObject entities = obj.getJSONObject("entities");
				JSONArray hashtagsArray = entities.getJSONArray("hashtags");
				ArrayList<String> hashtags = new ArrayList<String>();
				for(int i=0; i<hashtagsArray.length(); i++){
					JSONObject hashtagObject = hashtagsArray.getJSONObject(i);
					hashtags.add(hashtagObject.getString("text"));
				}

				System.out.println("Entered main loop");
				Tweet tweet = new Tweet(d1, hashtags);
				System.out.println("currentTweets.size: "+currentTweets.size());
				if(currentTweets.size() != 0){
					System.out.println("tweet.time: "+tweet.time);
					System.out.println("currentTweetsTime: "+currentTweets.last().time);
					if(findTimeDiff(tweet.time, currentTweets.last().time)<0){
						System.out.println("entered loop1");
						//the tweet is out of order
						if(findTimeDiff(currentTweets.last().time, tweet.time)<=60){
							currentTweets.add(tweet);
						}else{
							computeDegree(graph, degrees, bufferedWriter);
							continue;
						}
					}else{
						//the tweet is not out of order
						System.out.println("entered loop2");
						Iterator itr = currentTweets.iterator();
						Tweet tempTweet;
						while(itr.hasNext()&&(findTimeDiff(tweet.time, (tempTweet=(Tweet)itr.next()).time)>60)){
							System.out.println("entered loop3");
							ArrayList<String> hashTags = tempTweet.hashtags;
							for(int j=0; j<hashTags.size(); j++){
								System.out.println("Entered remove loop");
								Node fromNode = currentHashTags.get(hashTags.get(j));
								fromNode.print();
								for(int k=j+1; k<hashTags.size(); k++){
									String forward = hashTags.get(j)+hashTags.get(k);
									String reverse = hashTags.get(k)+hashTags.get(j);
									boolean removeEdge = false;
									if(currentEdges.containsKey(forward)){
										int count = currentEdges.get(forward);
										count--;
										if(count == 0){
											currentEdges.remove(forward);
											removeEdge = true;
										}
									}else if(currentEdges.containsKey(reverse)){
										int count = currentEdges.get(reverse);
										count--;
										if(count == 0){
											currentEdges.remove(reverse);
											removeEdge = true;
										}
									}else{
										//this part should not be reached
										System.out.println("this part should not be reached");
									}
									if(removeEdge == true){
										Node toNode = currentHashTags.get(hashTags.get(k));
										fromNode.connections.remove(hashTags.get(k));
										toNode.connections.remove(hashTags.get(j));
									}
								}
								fromNode.print();
								if(fromNode.connections.size()==0) {
									//remove the node from the graph
									graph.nodes.remove(hashTags.get(j));
								}
								
							}
							itr.remove();
						}
						currentTweets.add(tweet);
					}
				}else {
					currentTweets.add(tweet);
				}
					//check if the hashtags of the tweets already exist in the graph. Create new nodes
					//for the new hashtags
					ArrayList<String> hashTags = tweet.hashtags;
					//Dont add if it to the graph is the only hashtag in the tweet
					if(hashTags.size()>1) {
						System.out.println(hashTags);
						Node tempNode = null;
						for(int j = 0; j< hashTags.size(); j++){
							if(currentHashTags.get(hashTags.get(j)) != null){
								
							}else{
								
								tempNode = new Node(hashTags.get(j));
								currentHashTags.put(hashTags.get(j), tempNode);
								graph.nodes.put(hashTags.get(j),tempNode);
							}
						}
						
						for(int j=0; j<hashTags.size(); j++){ 
							Node fromNode = currentHashTags.get(hashTags.get(j));
							for(int k=j+1; k<hashTags.size(); k++){
								//to store in currentedges
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
								//check if there is already an edge
								
								if(!fromNode.connections.containsKey(hashTags.get(k))){
									fromNode.connections.put(hashTags.get(k), toNode);
								}
								System.out.println(hashTags.get(j));
					
								if(!toNode.connections.containsKey(hashTags.get(j))){
									toNode.connections.put(hashTags.get(j), fromNode);
								}
							}
						}
					}
					//calculate the average degree of the graph
					System.out.println("currentHashTags: "+currentHashTags);
					graph.print();
					computeDegree(graph, degrees, bufferedWriter);
					System.out.println("degrees: "+degrees);
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
		System.out.println("degrees size: "+degrees.size());
		try {
			br.close();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return degrees;
	}
	
	public static void computeDegree(Graph graph, ArrayList<Double> degrees, BufferedWriter bw){
		DecimalFormat d2 = new DecimalFormat("###.##");
		d2.setRoundingMode(RoundingMode.DOWN);
		int nodesCount = 0;
		double degreeCount = 0;
		for(Map.Entry entry : graph.nodes.entrySet()){
			nodesCount++;
			HashMap<String, Node> edges = ((Node)entry.getValue()).connections;
			degreeCount += edges.size();
		}
		if(nodesCount != 0) degreeCount /= (double)nodesCount;
		System.out.println("degreeCount: "+degreeCount);
		degreeCount = Double.valueOf(d2.format(degreeCount));
		try {
			bw.write(String.valueOf(degreeCount));
			bw.write("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		degrees.add(degreeCount);
	}
	
	public static long findTimeDiff(Date d1, Date d2){
		long diff = d2.getTime() - d1.getTime();
	    long diffSeconds = diff / 1000;
	    return diffSeconds;
	}
}

