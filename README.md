# Kafka Streams Exactly Once Example Updated to Java 11 and Kafka 2.7.0

A demo of Kafka Streams and Kafka Table.

## Description

This is an adaptation of https://github.com/simplesteph/kafka-streams-course "bank-balance-exactly-once" example
It has been upgraded to Java11 and Kafka Streams 2.7.0
Messages a managed with POJOs that have custom Serializer and Deserializer

## Getting Started

### Dependencies

* Describe any prerequisites, libraries, OS version, etc., needed before installing program.
* Java 11
* Kafka Streams 2.7.0
* Lombok Project

### Installing

* Run Kafka and Zookeeper platform
* This version has been tested using a Confluent platform running in a Windows 10 and Docker Desktop
* configure an inputTopic and an outputTopic

### Executing program

* How to run the program
* Create input topic with one partition to get full ordering
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-transactions
```
* Create output log compacted topic 
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-balance-exactly-once --config cleanup.policy=compact
```
* Start the *Producer, see how publish messages in the inputTopic
* Start the main program, see how read messages from the inputTopic and publish them in the outputTopic grouping customer transactions into customer balance

## Help

Any advise for common problems or issues.
```
command to run if program contains helper info
```

## Authors

Contributors names and contact info

ex. Miguel Salas  
ex. [@msalaslo](https://github.com/msalaslo)

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the GNU License 

## Acknowledgments

Inspiration, code snippets, etc.
* [kafka streams course](https://github.com/simplesteph/kafka-streams-course "bank-balance-exactly-once")

