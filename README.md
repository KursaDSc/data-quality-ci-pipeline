# Data Quality CI Pipeline

CI/CD pipeline for automated data quality validation. Uses GitHub Actions to validate Amazon sales data with Great Expectations & Pydantic on every commit. Includes Slack notifications for data quality issues.

## 🚀 Features

- **Automated Data Validation** - Great Expectations and Pydantic validations
- **CI/CD Integration** - Runs on every push and pull request
- **Slack Notifications** - Real-time alerts for data quality issues
- **Row-Level Validation** - Pydantic model-based validation
- **Dataset Validation** - Great Expectations for dataset-level checks

## 📊 Validation Rules

### Great Expectations
- Order ID must not be null
- Order ID must be unique
- Quantity must be non-negative
- Amount must be non-negative
- Status must be in allowed values

### Pydantic (Row-Level)
- Order ID: string, not empty
- Quantity: integer ≥ 0
- Amount: float ≥ 0
- Currency: must be "INR"
- Ship Country: must be "IN"
- Date: must match format (e.g., %m-%d-%y)

## 🔧 Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/data-quality-ci-pipeline.git
   cd data-quality-ci-pipeline