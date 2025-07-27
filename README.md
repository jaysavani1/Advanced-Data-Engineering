# Olympic Analytics Platform

<img width="645" height="355" alt="image" src="https://github.com/user-attachments/assets/80ecf01c-e6aa-4f6c-ab87-bbe6de4eda55" />


## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Data Lake     │    │   Analytics     │
│                 │    │                 │    │                 │
│ • CSV Files     │───▶│ • Raw Zone      │───▶│ • Databricks    │
│ • APIs          │    │ • Processed Zone│    │ • Synapse       │
│ • Streams       │    │ • Curated Zone  │    │ • Power BI      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Ingestion     │    │ Transformation  │    │ Visualization   │
│                 │    │                 │    │                 │
│ • Event Hubs    │    │ • PySpark       │    │ • Dashboards    │
│ • Kafka         │    │ • Data Quality  │    │ • Reports       │
│ • Batch Load    │    │ • Aggregations  │    │ • Alerts        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Features

- **Real-time Data Ingestion**: Apache Kafka and Azure Event Hubs integration
- **Scalable Data Processing**: Azure Databricks with PySpark
- **Data Lake Architecture**: Multi-zone storage (Raw, Processed, Curated)
- **Data Quality & Monitoring**: Automated validation and alerting
- **CI/CD Pipeline**: Azure DevOps integration
- **Interactive Dashboards**: Power BI visualizations
- **Configuration Management**: Environment-based configuration
- **Logging & Monitoring**: Comprehensive observability

## 📁 Project Structure

```
olympic-analytics-platform/
├── config/                     # Configuration files
│   ├── environments/           # Environment-specific configs
│   └── templates/              # Configuration templates
├── src/                        # Source code
│   ├── ingestion/              # Data ingestion modules
│   ├── transformation/         # Data transformation logic
│   ├── quality/                # Data quality checks
│   ├── utils/                  # Utility functions
│   └── tests/                  # Unit tests
├── notebooks/                  # Databricks notebooks
├── pipelines/                  # Azure DevOps pipelines
├── terraform/                  # Infrastructure as Code
├── docs/                       # Documentation
└── data/                       # Sample data and schemas
```

## 🛠️ Technology Stack

- **Cloud Platform**: Microsoft Azure
- **Data Ingestion**: Azure Event Hubs, Apache Kafka
- **Data Processing**: Azure Databricks, PySpark
- **Data Storage**: Azure Data Lake Storage Gen2
- **Data Warehouse**: Azure Synapse Analytics
- **Orchestration**: Azure Data Factory
- **Visualization**: Power BI
- **CI/CD**: Azure DevOps
- **Infrastructure**: Terraform
- **Monitoring**: Azure Monitor, Application Insights

## 🚀 Quick Start

### Prerequisites

- Azure Subscription
- Python 3.8+
- Azure CLI
- Terraform
- Docker (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd olympic-analytics-platform
   ```

2. **Set up environment**
   ```bash
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

3. **Configure Azure resources**
   ```bash
   # Login to Azure
   az login
   
   # Deploy infrastructure
   cd terraform
   terraform init
   terraform plan
   terraform apply
   ```

4. **Set up configuration**
   ```bash
   # Copy configuration template
   cp config/templates/config.template.yaml config/environments/dev.yaml
   
   # Update with your Azure resource details
   # Update connection strings, endpoints, etc.
   ```

5. **Run the pipeline**
   ```bash
   # Start data ingestion
   python src/ingestion/main.py
   
   # Run transformations
   python src/transformation/main.py
   ```

## 📊 Data Models

### Core Entities

- **Athletes**: Olympic athletes with performance data
- **Teams**: National teams and their compositions
- **Medals**: Medal counts and rankings by country
- **Events**: Olympic events and disciplines
- **Coaches**: Team coaches and their associations

### Data Quality Rules

- No null values in required fields
- Valid country codes
- Consistent date formats
- Medal counts validation
- Gender distribution validation

## 🔧 Configuration

The platform uses environment-based configuration:

```yaml
# config/environments/dev.yaml
azure:
  subscription_id: "your-subscription-id"
  resource_group: "olympic-analytics-rg"
  
databricks:
  workspace_url: "https://your-workspace.azuredatabricks.net"
  cluster_id: "your-cluster-id"
  
storage:
  account_name: "yourstorageaccount"
  container_raw: "raw-data"
  container_processed: "processed-data"
  
event_hubs:
  namespace: "olympic-events"
  hub_name: "olympics-data"
```

## 📈 Monitoring & Alerting

- **Data Quality Metrics**: Automated validation results
- **Pipeline Performance**: Processing times and throughput
- **Error Tracking**: Failed records and exceptions
- **Resource Utilization**: CPU, memory, and storage usage

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:
- Create an issue in the repository
- Check the [documentation](docs/)
- Review the [troubleshooting guide](docs/troubleshooting.md)

## 🔄 Version History

- **v1.0.0**: Initial production release
- **v0.9.0**: Beta release with core functionality
- **v0.8.0**: Alpha release with basic pipeline 
