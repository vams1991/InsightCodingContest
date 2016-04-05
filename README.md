Instructing to run the project:

- run the 'run.sh' script file. The output will be generated in the 'output.txt' file in tweet_output folder.
- the src file contains the average_degree.java file. It also includes a average_degree.jar file(created a jar file since i have used     dependencies). The jar file is essential to run the 'run.sh' script.

Dependencies Used:

org.json

How to build the maven project using eclipse:

- In the Eclipse IDE, navigate to File > New > Other… in order to bring up the project creation wizard.
- Scroll to the Maven folder, open it, and choose Maven Project. Then choose Next.
- Uncheck the 'Create a simple project' and click next;
- Now, you will need to enter information regarding the Maven Project you are creating. Enter any random name for Group Id say 'Insight'.For the Artifact Id enter the project’s name say 'average_degree'. The version is up to your discretion as is the packing and other fields. Leave the Parent Project section as is. And click Finish.
- Place 'average_degree.java' file in '/src/main/java' folder.
- Open the pom.xml file to view the structure Maven has set up. In this file, you can see the information entered in Step 4. You may also use the tabs at the bottom of the window to change to view Dependencies, the Dependency Hierarchy, the Effective POM, and the raw xml code for the pom file in the pom.xml tab. 

- Add this dependency in the pom file
	<dependency>
		<groupId>org.json</groupId>
		<artifactId>json</artifactId>
		<version>20090211</version>
	</dependency>

- Right click on the project, select 'Run As' and the select 'Maven Install'. Maven will download the dependency automatically. 

The project is ready.
