TweetsWriter:

This application writes the *sample* twitter stream into a Kafka topic.

To run this, follow the steps given below:

1)  Compile & package:  mvn clean package
2)  Create an application.properties in the 'root' directory with following properties:

authConsumerKey=
authConsumerSecret=
authAccessToken=
authAccessTokenSecret=

3)  Start consuming stream as follows:

java -jar TweetsWriter-1.0-SNAPSHOT.jar


