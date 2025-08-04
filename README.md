# Real-Time Data Ingestion Pipeline for User Events using Kafka, Python, and PostgreSQL

This pipeline simulates and processes user activity events in real time using Apache Kafka and PostgreSQL. It supports late-arriving events, event corrections, and deletions. It includes validation and integration testing to ensure data correctness and observability metrics for ingestion monitoring.

## a. Setup & Run Instructions

## 1. Prerequisites
- [Python 3.13.5](https://www.python.org/downloads/windows/](https://www.python.org/ftp/python/3.13.5/python-3.13.5-amd64.exe))
- [PostgreSQL](https://sbp.enterprisedb.com/getfile.jsp?fileid=1259622&_gl=1*lzk3tu*_gcl_au*MTE3NTY4NzgxMy4xNzU0MjExMTcz*_ga*R0ExLjEuR0ExLjEuR0ExLjEuR0ExLjEuODkzNjQ0OTU4LjE3NTQyMTExNzM.*_ga_ND3EP1ME7G*czE3NTQyMTExNzIkbzEkZzEkdDE3NTQyNDMxNzYkajYwJGwwJGgxNDg5MDg3Mzk1)
- Kafka (local setup)
  #### 1. Install Java (JDK 11 or later)
    - Download: https://adoptium.net/  
    - Add `JAVA_HOME` to environment variables.

    #### Set `JAVA_HOME` (important)

    After installing:

    A. Press `Win + S` -> Search for “Environment Variables”
  
    B. Under **System Variables**, click **New**:
        - **Variable name:** `JAVA_HOME`  
        - **Variable value:** e.g., `C:\Program Files\Eclipse Adoptium\jdk-17.0.x`

    C. Edit the **Path** variable:
        - Add: `%JAVA_HOME%\bin`

    #### 2. Install Apache Kafka (Binary Release)

    - [Download Kafka:](https://kafka.apache.org/downloads)
    - Choose the latest binary release (e.g., "Scala 2.13 – Kafka 3.x").

    #### Steps to Set Up Kafka on Windows

    #### A. Extract Kafka
    Extract the downloaded Kafka ZIP file (e.g., `kafka_2.13-3.6.0`) to a folder like: 
      <pre lang="markdown">  C:\kafka  </pre>

    #### B. Start Zookeeper (Kafka’s dependency)
    Kafka uses Zookeeper to manage the cluster. Run this in **Command Prompt**:
    <pre lang="markdown"> cd C:\kafka.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties </pre>

    Keep this terminal open.

    #### C. Start Kafka Broker
    In a new Command Prompt window:

    <pre lang="markdown"> cd C:\kafka.\bin\windows\kafka-server-start.bat .\config\server.properties </pre>

    #### Test Kafka Setup

    #### D. Create a Topic
    <pre lang="markdown"> cd C:\kafka.\bin\windows\kafka-topics.bat --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 </pre>

    #### E. Start a Producer
    <pre lang="markdown"> .\bin\windows\kafka-console-producer.bat --topic test-topic --bootstrap-server localhost:9092 </pre>

    Type messages and hit Enter.

    #### F. Start a Consumer
    Open another terminal:
    <pre lang="markdown"> .\bin\windows\kafka-console-consumer.bat --topic test-topic --from-beginning --bootstrap-server localhost:9092 </pre>

    You should see the messages typed in the producer console.

    #### If error encounter wmic command (C:\kafka>.\bin\windows\kafka-server-start.bat .\config\server.properties
    'wmic' is not recognized as an internal or external command,operable program or batch file.)


    #### Step-by-Step Fix for 'wmic' is not recognized in Kafka on Windows


    #### 1. Set the broker ID manually
    Edit server.properties to avoid needing wmic.

   <pre lang="markdown"> Open the file: C:\kafka\config\server.properties </pre>

    #### 2. Add or edit the following line:
    <pre lang="markdown"> Properties: broker.id=1 </pre>

    #### 3. Save the file, close all and re-run:
    <pre lang="markdown"> .\bin\windows\kafka-server-start.bat .\config\server.properties  </pre>


