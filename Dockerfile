FROM openjdk:11

COPY config/ /opt/smarttrie/config/
COPY target/scala-2.13/SmartTrie.jar /opt/smarttrie/SmartTrie.jar

WORKDIR /opt/smarttrie/

ENTRYPOINT ["java", "-cp", "SmartTrie.jar"]