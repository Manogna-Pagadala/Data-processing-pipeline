# Real-Time Data Pipeline with Kubernetes, Kafka, and Neo4j

A containerized data processing pipeline built using **Apache Kafka, Kubernetes, Neo4j, Docker, Helm, and Python**. The system ingests NYC Yellow Taxi trip data, streams it through Kafka, stores it in a Neo4j graph database, and performs graph analytics using Breadth-First Search (BFS) and PageRank.

The pipeline is deployed on a local Kubernetes cluster using Minikube and integrates Kafka Connect with the Neo4j Sink Connector for automated data ingestion.

## Overview

This project demonstrates the design and implementation of a distributed data pipeline for streaming data ingestion, graph storage, and analytics.

The system follows four main stages:

1. **Data Ingestion:** A Python producer reads the NYC Yellow Taxi trip dataset and publishes records to an Apache Kafka topic.
2. **Stream Processing:** Kafka receives and buffers incoming records, allowing the producer and downstream database to operate independently.
3. **Graph Storage:** Kafka Connect and the Neo4j Sink Connector transfer data from Kafka into Neo4j.
4. **Graph Analytics:** Neo4j's Graph Data Science (GDS) library executes BFS and PageRank to analyze relationships within the stored graph.

All major pipeline components run within a Kubernetes environment managed through Minikube.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data ingestion and pipeline testing |
| Apache Kafka | Distributed event streaming and message buffering |
| Apache ZooKeeper | Kafka broker coordination |
| Kafka Connect | Integration between Kafka and Neo4j |
| Neo4j Sink Connector | Transferring Kafka records into Neo4j |
| Neo4j | Graph database for storing and querying data |
| Neo4j Graph Data Science | BFS and PageRank graph analytics |
| Kubernetes | Container orchestration |
| Minikube | Local Kubernetes cluster |
| Helm | Neo4j deployment and configuration |
| Docker | Packaging the Kafka Connect service |
| YAML | Kubernetes resource configuration |
| kubectl | Kubernetes deployment and service management |

## System Architecture

```text
                   NYC Yellow Taxi Trip Dataset
                                |
                                v
                        Python Data Producer
                                |
                                v
                         Apache Kafka Topic
                                |
                                v
                          Kafka Connect
                       (Custom Docker Image)
                                |
                                v
                        Neo4j Sink Connector
                                |
                                v
                         Neo4j Database
                                |
                                v
                     Neo4j Graph Data Science
                                |
                       +--------+--------+
                       |                 |
                       v                 v
                      BFS             PageRank
                       |                 |
                       +--------+--------+
                                |
                                v
                       Python Test Scripts
```

**Deployment environment:** Kafka, ZooKeeper, Kafka Connect, and Neo4j run within a Minikube Kubernetes cluster. Local Python scripts communicate with the services through Kubernetes port forwarding.

## Key Features

### 1. Streaming Data Ingestion with Apache Kafka

The pipeline uses a Python producer to read the NYC Yellow Taxi trip dataset and publish records to Kafka.

Kafka provides an intermediate streaming layer between the data source and Neo4j, decoupling data production from database ingestion.

The Kafka broker is deployed using Kubernetes YAML configurations, including service definitions, environment variables, and resource settings.

### 2. Kafka and ZooKeeper Deployment on Kubernetes

Apache Kafka and ZooKeeper are deployed as containerized services inside Minikube.

The Kafka configuration includes:

- Kafka broker ID configuration.
- ZooKeeper connectivity on port `2181`.
- Internal Kafka communication through port `29092`.
- External access through port `9092`.
- Kubernetes services for communication between components.

The implementation uses the `confluentinc/cp-kafka:7.3.3` container image.

### 3. Neo4j Graph Database Deployment

A standalone Neo4j instance is deployed within Kubernetes using Helm.

The deployment includes:

- Resource allocation configuration.
- Database authentication.
- Neo4j Graph Data Science library.
- Kubernetes service exposure for database access.

Neo4j uses port `7687` for Bolt connections and port `7474` for its HTTP interface.

### 4. Kafka Connect and Neo4j Integration

The pipeline uses Kafka Connect with the Neo4j Sink Connector to transfer streaming data into the graph database.

A custom Docker image packages the Kafka Connect worker, Neo4j connector plugin, connector configuration, and startup script.

The connector configuration specifies Kafka bootstrap servers, worker settings, and Neo4j integration parameters.

Kafka Connect exposes its REST API on port `8083`.

### 5. Graph Analytics Using Neo4j GDS

Once the data is ingested into Neo4j, graph analytics are performed using the Graph Data Science library.

**Breadth-First Search (BFS)**

BFS is used to explore graph relationships and traverse connected nodes. The project uses BFS-based testing to validate graph structure and traversal behavior.

**PageRank**

PageRank evaluates the relative importance of nodes based on their connections within the graph.

These algorithms demonstrate how graph analytics can be applied to data processed through a streaming pipeline.

### 6. Containerized Deployment

The pipeline components are deployed within a Kubernetes cluster, allowing the services to be configured and managed independently.

Kubernetes manifests define the required deployments, services, environment variables, and resource limits.

Helm is used for Neo4j installation, while a custom Docker image is used for Kafka Connect.

## Implementation Details

### Python Data Producer

The producer reads the NYC Yellow Taxi trip dataset and sends records to an Apache Kafka topic.

It provides the initial data source for the pipeline.

### Kafka and ZooKeeper

Kafka handles incoming data records and makes them available to downstream consumers.

ZooKeeper coordinates the Kafka broker in the version and configuration used by this project.

### Kafka Connect

Kafka Connect transfers data from the Kafka topic to Neo4j using the Neo4j Sink Connector.

The connector is packaged in a custom Docker image and deployed through Kubernetes.

### Neo4j Database

Neo4j stores the ingested data in a graph representation, enabling relationship-based queries and analytics.

The database is deployed as a standalone instance using Helm.

### Graph Analytics and Testing Scripts

Python scripts validate data ingestion and execute graph analytics operations using Neo4j.

The tests cover BFS traversal and PageRank analysis.

## Deployment and Setup

### Prerequisites

- Python 3
- Docker
- Minikube
- Kubernetes CLI (`kubectl`)
- Helm
- Apache Kafka and ZooKeeper deployment configurations
- Neo4j Helm chart
- Neo4j Sink Connector
- NYC Yellow Taxi trip dataset

### Step 1: Start Minikube

Start the local Kubernetes cluster:

```bash
minikube start
```

Verify that the cluster is running:

```bash
kubectl get nodes
```

### Step 2: Deploy ZooKeeper and Kafka

Apply the Kubernetes YAML manifests for ZooKeeper and Kafka.

```bash
kubectl apply -f <zookeeper-manifest>.yaml
kubectl apply -f <kafka-manifest>.yaml
```

Verify the deployments:

```bash
kubectl get pods
kubectl get services
```

The Kafka configuration used in the project includes:

```text
KAFKA_BROKER_ID=1
KAFKA_ZOOKEEPER_CONNECT=zookeeper-service:2181
```

The advertised listeners are configured for communication inside the cluster and through local port forwarding.

### Step 3: Deploy Neo4j

Install Neo4j using Helm with the required resource settings, authentication, and Graph Data Science configuration.

```bash
helm install neo4j <neo4j-chart> -f <neo4j-values>.yaml
```

Apply the Neo4j service configuration if it is maintained separately:

```bash
kubectl apply -f neo4j-service.yaml
```

Verify the deployment:

```bash
kubectl get pods
kubectl get services
```

### Step 4: Build and Deploy Kafka Connect

Build the custom Docker image containing Kafka Connect and the Neo4j Sink Connector.

```bash
docker build -t kafka-neo4j-connect .
```

Make the image accessible to the Kubernetes cluster using the image-loading or registry workflow configured for Minikube.

Deploy the connector:

```bash
kubectl apply -f kafka-neo4j-connector.yaml
```

The connector service exposes the Kafka Connect REST API on port `8083`.

### Step 5: Configure Service Access

Use Kubernetes port forwarding to access Kafka and Neo4j from local Python scripts.

For example, using the service names described in the project:

```bash
kubectl port-forward svc/kafka-service 9092:9092
```

In another terminal:

```bash
kubectl port-forward svc/neo4j-service 7687:7687
```

The exact service names and port mappings should match the Kubernetes manifests in the repository.

### Step 6: Run the Data Producer

Configure the Python producer with the Kafka bootstrap address, topic, and NYC Yellow Taxi dataset location.

Run the producer script to publish records to Kafka.

The Kafka Connect worker and Neo4j Sink Connector then transfer the records into the Neo4j database.

### Step 7: Execute Graph Analytics

After confirming that data has been ingested into Neo4j, run the Python testing scripts.

The tests execute BFS and PageRank operations through the Neo4j Graph Data Science library and validate the resulting graph analytics.

## Testing and Evaluation

The pipeline was tested to verify communication between its components and the correctness of graph analytics operations.

### Test 1: Kafka Data Ingestion

A Python producer loaded the NYC Yellow Taxi trip dataset and published records to Kafka.

The test verified that records were successfully received by the Kafka broker.

### Test 2: Kafka-to-Neo4j Integration

Kafka Connect and the Neo4j Sink Connector transferred data from Kafka into Neo4j.

The test verified that the data was available in the graph database for querying and analysis.

### Test 3: Breadth-First Search

BFS was executed using the Neo4j Graph Data Science library to explore graph relationships.

The results were checked to validate graph traversal functionality.

### Test 4: PageRank

PageRank was executed on the graph data to identify nodes with higher relative importance.

The results were checked against expected outcomes.

### Results

The project successfully demonstrated:

- Deployment of Kafka, ZooKeeper, Kafka Connect, and Neo4j within Minikube.
- Data ingestion from a Python producer into Kafka.
- Data transfer from Kafka into Neo4j through the sink connector.
- Graph data querying and analysis using Neo4j GDS.
- Successful execution of BFS and PageRank.
- End-to-end pipeline validation using Python scripts.

## Project Outcome

This project demonstrates the integration of distributed event streaming, container orchestration, and graph database technologies into a unified data pipeline.

The implementation covers:

- Building a streaming data ingestion pipeline with Kafka.
- Deploying distributed services using Kubernetes and Minikube.
- Integrating Kafka and Neo4j through Kafka Connect.
- Creating custom Docker images for connector deployment.
- Configuring Neo4j using Helm.
- Performing graph analytics with BFS and PageRank.
- Testing the complete data flow from ingestion to graph analysis.

The architecture provides a foundation for applications involving streaming analytics, graph-based data exploration, recommendation systems, and relationship analysis.

## Technologies Used

`Python` · `Apache Kafka` · `Apache ZooKeeper` · `Kafka Connect` · `Neo4j` · `Neo4j GDS` · `Kubernetes` · `Minikube` · `Docker` · `Helm` · `YAML` · `Graph Analytics` · `BFS` · `PageRank`
