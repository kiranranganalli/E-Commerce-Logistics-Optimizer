
# E-Commerce Logistics Optimizer

## Project Overview

The **E-Commerce Logistics Optimizer** is a large-scale data engineering and analytics solution built to simulate package routing across a virtual fulfillment network and optimize cost-to-serve. It integrates routing algorithms, high-volume data ingestion, transformation workflows, and interactive dashboards to help logistics teams make informed decisions at scale.

This project mimics the operations of a global e-commerce supply chain and offers insights into package delivery latency, optimal path discovery, regional load distribution, and cost-saving opportunities.

---

## Key Features

- **Simulated Routing:** Over 1 million synthetic package records routed across 500+ virtual fulfillment nodes.
- **Routing Algorithms:** Applied Dijkstra’s algorithm to compute least-cost and shortest-distance delivery paths.
- **ETL Pipeline:** Data ingested from partitioned S3 files and transformed using AWS Glue (PySpark).
- **Warehouse Metrics:** Daily delivery statistics, node workloads, and failure patterns stored in Amazon Redshift.
- **Scenario Simulation:** What-if planners using Tableau dashboards for rerouting, blackout simulations, and volume surges.
- **Infrastructure:** Designed for distributed execution and storage, ensuring scalability to billions of packages per day.

---

## Tech Stack

- **Cloud Services:** AWS S3, Redshift, Glue, Lambda
- **Processing:** PySpark, Dask, Pandas
- **Visualization:** Tableau, Amazon QuickSight
- **Data Format:** Parquet, CSV, JSON

---

## Data Flow Architecture

1. **Data Generation**
   - Synthetic generation of package metadata: ID, origin, destination, weight, SLA category, and timestamps.

2. **Ingestion Layer**
   - Partitioned files ingested from S3 using Glue jobs and stored in temporary Spark tables.

3. **Routing Computation**
   - Routing jobs calculate optimal paths using graph traversal techniques on fulfillment center graph.

4. **Redshift Storage**
   - Final metrics are aggregated and stored in Redshift tables, supporting historical trend analysis.

5. **Dashboards & Monitoring**
   - Tableau dashboard to simulate what-if scenarios (blackouts, cost spikes).
   - Built-in monitoring for SLA breaches and path failures.

---

## Use Cases

- **Logistics Optimization**
  - Reduce delivery costs by minimizing miles traveled and load balancing.

- **Scenario Planning**
  - Visualize disruptions (e.g., regional strikes) and automatically re-route deliveries.

- **Operational Insights**
  - Spot overloaded hubs, delayed shipments, and low-performance lanes.

- **ML Model Training**
  - Use aggregated metrics to train ETA and delay prediction models.

---

## Sample Metrics

| Metric                  | Description                                 |
|------------------------|---------------------------------------------|
| Avg Path Length (km)   | Mean distance packages travel across graph |
| Cost-to-Serve ($)      | Cost of transporting package from origin to destination |
| Load by Node           | Number of packages assigned to fulfillment centers |
| SLA Compliance (%)     | % of packages delivered within SLA         |
| Delivery Time (mins)   | Time from dispatch to delivery              |

---

## Setup Instructions

1. Install requirements using pip:
   ```bash
   pip install pandas networkx boto3 sqlalchemy
   ```

2. Run ETL pipeline:
   ```bash
   python etl_pipeline.py
   ```

3. Launch routing engine:
   ```bash
   python routing_engine.py
   ```

4. Access dashboard in Tableau using Redshift connection.

---

## Future Work

- Real-time updates using Kinesis + Lambda
- Support for reverse logistics (returns)
- Integration with real-world routing APIs (e.g., OpenRouteService)

---

## Authors

- Kiran Ranganalli - Data Engineer | Logistics and Routing Intelligence
- OpenAI Collaboration for Content Simulation

---

© 2025 - E-Commerce Logistics Intelligence Systems
