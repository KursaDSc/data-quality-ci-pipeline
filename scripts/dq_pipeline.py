#!/usr/bin/env python3
"""
Data Quality Validation Pipeline for GitHub Actions
Combines Great Expectations and Pydantic validations
Exits with non-zero code on validation failures
"""

import pandas as pd
import sys
import os
import requests
import json

def validate_with_great_expectations(df):
    """Run Great Expectations dataset validation"""
    try:
        import great_expectations as gx
        from great_expectations.core.batch import RuntimeBatchRequest
        
        print("🔍 Running Great Expectations validation...")
        
        # Initialize GE context
        context = gx.get_context()
        
        # Configure datasource
        datasource_config = {
            "name": "validation_datasource",
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                "default_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                }
            }
        }
        context.add_datasource(**datasource_config)
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="validation_datasource",
            data_connector_name="default_connector",
            data_asset_name="amazon_orders",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": "ci_pipeline"},
        )
        
        # Create expectation suite
        suite_name = "ci_validation_suite"
        context.add_expectation_suite(suite_name)
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )
        
        # Define expectations
        validator.expect_column_values_to_not_be_null(column="Order ID")
        validator.expect_column_values_to_be_unique(column="Order ID")
        validator.expect_column_values_to_be_between(column="Qty", min_value=0)
        validator.expect_column_values_to_be_between(column="Amount", min_value=0)
        
        allowed_statuses = ["Delivered", "Shipped", "Processing", "Cancelled"]
        validator.expect_column_values_to_be_in_set(
            column="Status", 
            value_set=allowed_statuses
        )
        
        # Run validation
        results = validator.validate()
        return results
        
    except Exception as e:
        print(f"❌ Great Expectations error: {e}")
        return None

def validate_with_pydantic(df):
    """Run Pydantic row-level validation"""
    try:
        from pydantic import BaseModel, field_validator
        from datetime import datetime
        
        print("🔍 Running Pydantic row-level validation...")
        
        class AmazonOrder(BaseModel):
            order_id: str
            qty: int
            amount: float
            currency: str
            ship_country: str
            date: str
            
            @field_validator('order_id')
            def order_id_not_empty(cls, v):
                if not v or v.strip() == "":
                    raise ValueError('Order ID cannot be empty')
                return v
            
            @field_validator('qty')
            def qty_non_negative(cls, v):
                if v < 0:
                    raise ValueError('Quantity must be ≥ 0')
                return v
            
            @field_validator('amount')
            def amount_non_negative(cls, v):
                if v < 0:
                    raise ValueError('Amount must be ≥ 0')
                return v
            
            @field_validator('currency')
            def currency_must_be_inr(cls, v):
                if v != "INR":
                    raise ValueError('Currency must be INR')
                return v
            
            @field_validator('ship_country')
            def country_must_be_india(cls, v):
                if v != "IN":
                    raise ValueError('Ship country must be IN')
                return v
            
            @field_validator('date')
            def validate_date_format(cls, v):
                try:
                    datetime.strptime(v, "%m-%d-%Y")
                    return v
                except ValueError:
                    raise ValueError('Invalid date format')
        
        # Validate each row
        valid_rows = []
        invalid_rows = []
        
        for index, row in df.iterrows():
            try:
                # Map CSV columns to Pydantic model fields
                order = AmazonOrder(
                    order_id=row.get('Order ID', ''),
                    qty=row.get('Qty', 0),
                    amount=row.get('Amount', 0.0),
                    currency=row.get('currency', ''),
                    ship_country=row.get('ship-country', ''),
                    date=row.get('Date', '')
                )
                valid_rows.append(row.to_dict())
            except Exception as e:
                invalid_row = row.to_dict()
                invalid_row['validation_error'] = str(e)
                invalid_rows.append(invalid_row)
        
        return {
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'total_rows': len(df),
            'valid_count': len(valid_rows),
            'invalid_count': len(invalid_rows)
        }
        
    except Exception as e:
        print(f"❌ Pydantic error: {e}")
        return None

def send_slack_alert(webhook_url, validation_results):
    """Send Slack notification for validation failures"""
    try:
        message = {
            "attachments": [
                {
                    "color": "#FF0000",
                    "title": "❌ Data Quality Validation Failed - GitHub Actions",
                    "fields": [
                        {
                            "title": "Failed Expectations",
                            "value": str(validation_results.get('failed_expectations', 0)),
                            "short": True
                        },
                        {
                            "title": "Invalid Rows", 
                            "value": str(validation_results.get('invalid_rows', 0)),
                            "short": True
                        },
                        {
                            "title": "Repository",
                            "value": "data-quality-ci-pipeline",
                            "short": True
                        }
                    ]
                }
            ]
        }
        
        response = requests.post(webhook_url, json=message)
        if response.status_code == 200:
            print("✅ Slack notification sent")
        else:
            print(f"❌ Slack notification failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error sending Slack: {e}")

def main():
    """Main validation pipeline"""
    print("🚀 Starting Data Quality Validation Pipeline...")
    
    # Get Slack webhook from environment variable (set by GitHub Actions)
    slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
    
    try:
        # Load data
        df = pd.read_csv('data/amazon_orders_sample.csv')
        print(f"📊 Loaded dataset: {len(df)} rows")
        
        validation_results = {
            'ge_success': False,
            'pydantic_success': False,
            'failed_expectations': 0,
            'invalid_rows': 0
        }
        
        # Run Great Expectations validation
        ge_results = validate_with_great_expectations(df)
        if ge_results:
            validation_results['ge_success'] = ge_results.success
            validation_results['failed_expectations'] = len([
                r for r in ge_results.results if not r.success
            ])
        
        # Run Pydantic validation
        pydantic_results = validate_with_pydantic(df)
        if pydantic_results:
            validation_results['pydantic_success'] = pydantic_results['invalid_count'] == 0
            validation_results['invalid_rows'] = pydantic_results['invalid_count']
            
            # ✅ FIX: Create CSV files for valid and invalid rows
            if pydantic_results['valid_rows']:
                valid_df = pd.DataFrame(pydantic_results['valid_rows'])
                valid_df.to_csv('valid_rows.csv', index=False)
                print(f"✅ Created valid_rows.csv with {len(valid_df)} rows")
            
            if pydantic_results['invalid_rows']:
                invalid_df = pd.DataFrame(pydantic_results['invalid_rows'])
                invalid_df.to_csv('invalid_rows.csv', index=False)
                print(f"✅ Created invalid_rows.csv with {len(invalid_df)} rows")
        
        # Print summary
        print("\n📊 VALIDATION SUMMARY")
        print("=" * 50)
        print(f"Great Expectations: {'✅ PASSED' if validation_results['ge_success'] else '❌ FAILED'}")
        print(f"Pydantic Validation: {'✅ PASSED' if validation_results['pydantic_success'] else '❌ FAILED'}")
        print(f"Failed Expectations: {validation_results['failed_expectations']}")
        print(f"Invalid Rows: {validation_results['invalid_rows']}")
        
        # Determine overall success
        overall_success = (
            validation_results['ge_success'] and 
            validation_results['pydantic_success']
        )
        
        # Send Slack notification if failed
        if not overall_success and slack_webhook:
            print("🔔 Sending Slack notification...")
            send_slack_alert(slack_webhook, validation_results)
        elif not overall_success:
            print("ℹ️  Slack webhook not configured - skipping notification")
        
        # Exit with appropriate code (CRITICAL for GitHub Actions)
        if overall_success:
            print("🎉 All validations passed!")
            sys.exit(0)
        else:
            print("❌ Validation failed - exiting with code 1")
            sys.exit(1)
            
    except Exception as e:
        print(f"💥 Pipeline error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()