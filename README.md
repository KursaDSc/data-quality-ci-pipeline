# Data Quality CI/CD Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-1.8.1-green)
![Pydantic](https://img.shields.io/badge/Pydantic-2.0-orange)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CI/CD-purple)
![Slack](https://img.shields.io/badge/Slack-Notifications-yellow)

A production-ready CI/CD pipeline for automated data quality validation. Validates Amazon sales data using Great Expectations and Pydantic on every commit, with intelligent environment-aware notifications.

## 🚀 Features

- **🔍 Automated Data Validation** - Great Expectations (dataset-level) + Pydantic (row-level)
- **⚡ CI/CD Integration** - GitHub Actions workflow on push, PR, and manual triggers
- **🎯 Environment-Aware Notifications** - Slack in production, console alerts locally
- **📊 CSV Export** - Automatic generation of valid_rows.csv and invalid_rows.csv
- **🚨 Non-Zero Exit Code** - Fails CI pipeline on validation errors
- **🔧 Configurable** - Environment-based configuration for different deployments

## 📊 Validation Rules

### Great Expectations (Dataset-Level)
- ✅ Order ID must not be null
- ✅ Order ID must be unique  
- ✅ Quantity must be non-negative
- ✅ Amount must be non-negative
- ✅ Status must be in allowed values

### Pydantic (Row-Level)
- ✅ Order ID: string, not empty
- ✅ Quantity: integer ≥ 0
- ✅ Amount: float ≥ 0
- ✅ Currency: must be "INR"
- ✅ Ship Country: must be "IN"
- ✅ Date: must match format (MM-DD-YY)

## 🏗️ Architecture

```mermaid
graph TB
    A[📁 CSV Data] --> B[🐼 Pandas Loading]
    B --> C[⚡ Great Expectations]
    B --> D[🎯 Pydantic Validation]
    
    C --> E[📊 Dataset Validation]
    D --> F[🔍 Row-Level Validation]
    
    E --> G[📈 Validation Results]
    F --> G
    
    G --> H{Environment Check}
    H -->|Production| I[📱 Slack Notification]
    H -->|Local| J[💻 Console Alert]
    
    G --> K[💾 CSV Export]
    K --> L[valid_rows.csv]
    K --> M[invalid_rows.csv]
    
    style A fill:#bbdefb,color:#000
    style B fill:#c8e6c9,color:#000
    style C fill:#fff9c4,color:#000
    style D fill:#fff9c4,color:#000
    style E fill:#e8f5e8,color:#000
    style F fill:#e8f5e8,color:#000
    style G fill:#fff3e0,color:#000
    style H fill:#e1f5fe,color:#000
    style I fill:#f3e5f5,color:#000
    style J fill:#e8f5e8,color:#000
    style L fill:#e8f5e8,color:#000
    style M fill:#ffebee,color:#000

    