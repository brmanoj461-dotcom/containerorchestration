# Container Orchestration Engine

A high-availability, lightweight container orchestration and multi-metric auto-scaling platform built for modern microservices.

---

## 🚀 Overview

**Container Orchestration Engine** is an enterprise-grade container management platform designed to automate application lifecycle management, dynamic scaling, and fault tolerance across containerized workloads.

Whether deploying single web services or multi-node microservices, the engine provides complete operational control through a unified CLI, RESTful API, and high-availability distributed controller architecture.

---

## ✨ Key Features

* **Intelligent Multi-Metric Auto-Scaling**: Dynamic horizontal scaling based on real-time CPU utilization, memory pressure, RPS (requests per second), latency thresholds, and active connections.
* **Dynamic Load Balancing**: Automated Nginx configuration management with health-aware traffic routing and SSL termination.
* **Proactive Health Monitoring**: Continuous container health probes paired with automated instance recovery and failover policies.
* **High-Availability (HA) Controller Cluster**: Distributed 3-node controller consensus using leader election to eliminate single points of failure and prevent split-brain scenarios.
* **Persistent State Management**: High-availability PostgreSQL backend featuring primary-replica data replication and instant recovery.
* **Declarative Deployments**: Simple, customizable YAML-based application specification files for seamless deployment setup.
* **Full Developer Interface**: Complete administrative control provided via both a CLI tool and an OpenAPI-compliant REST API.
* **Audit & Event Tracking**: Comprehensive event log streams and audit trails for detailed cluster activity monitoring.

---

## 📐 Architecture Overview

```text
                        +------------------------+
                        |  CLI / REST API Client |
                        +-----------+------------+
                                    |
                                    v
                        +------------------------+
                        |  Nginx Load Balancer   |
                        +-----------+------------+
                                    |
              +---------------------+---------------------+
              |                     |                     |
              v                     v                     v
     +------------------+  +------------------+  +------------------+
     | Controller Node 1|  | Controller Node 2|  | Controller Node 3|
     |    (Leader)      |  |    (Follower)    |  |    (Follower)    |
     +--------+---------+  +--------+---------+  +--------+---------+
              |                     |                     |
              +---------------------+---------------------+
                                    |
                                    v
                        +------------------------+
                        | High-Availability DB   |
                        | PostgreSQL Primary /   |
                        |        Replica         |
                        +------------------------+