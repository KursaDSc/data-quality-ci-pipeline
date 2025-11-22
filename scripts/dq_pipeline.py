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

def should_send_slack_notification():
    """
    Determine if Slack notification should be sent based on environment
    """
    # Check if running in GitHub Actions
    if os.getenv('GITHUB_ACTIONS') == 'true':
        return True
    
    # Check if running in production environment
    if os.getenv('ENVIRONMENT') == 'production':
        return True
    
    # Check if explicitly enabled for local development
    if os.getenv('SLACK_NOTIFICATIONS_ENABLED') == 'true':
        return True
    
    # Default: Don't send in local development
    return False

def handle_local_notification(validation_results):
    """Enhanced local notifications without Slack"""
    print("\n🔔 LOCAL DEVELOPMENT ALERT")
    print("=" * 50)
    print("Data quality issues detected during local development:")
    print(f"• Failed Expectations: {validation_results['failed_expectations']}")
    print(f"• Invalid Rows: {validation_results['invalid_rows']}")
    print("• Check files: valid_rows.csv and invalid_rows.csv")
    print("=" * 50)

def send_slack_alert(webhook_url, validation_results):
    """Send Slack notification only in appropriate environments"""
    
    environment = os.getenv('ENVIRONMENT', 'local')
    
    # Local development - use console notifications instead of Slack
    if not should_send_slack_notification():
        print("🔕 Slack notifications disabled for current environment")
        handle_local_notification(validation_results)
        return True  # Return success to avoid failing the pipeline
    
    if not webhook_url:
        print("❌ Slack webhook URL not configured")
        return False
    
    try:
        message = {
            "attachments": [
                {
                    "color": "#FF0000",
                    "title": "❌ Data Quality Validation Failed",
                    "fields": [
                        {
                            "title": "Environment",
                            "value": environment,
                            "short": True
                        },
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
                    ],
                    "footer": "GitHub Actions CI/CD Pipeline",
                    "ts": pd.Timestamp.now().timestamp()
                }
            ]
        }
        
        response = requests.post(webhook_url, json=message)
        if response.status_code == 200:
            print("✅ Slack notification sent successfully!")
            return True
        else:
            print(f"❌ Slack notification failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error sending Slack: {e}")
        return False

# DEBUG: DataFrame Info fonksiyonu (mevcut)
def debug_dataframe(df):
    """Debug dataframe structure"""
    print("🔍 DEBUG: DataFrame Info")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"First 2 rows:")
    print(df.head(2))
    print("=" * 50)

# Great Expectations validation fonksiyonu (mevcut - çalışan versiyon)
def validate_with_great_expectations(df):
    """Run Great Expectations validation using the working approach"""
    try:
        import great_expectations as gx
        
        print("🔍 Running Great Expectations validation...")
        
        # Create data context
        context = gx.get_context()

        # Add pandas datasource
        datasource = context.data_sources.add_pandas(name="amazon_pandas_datasource")

        # Create data asset
        data_asset = datasource.add_dataframe_asset(name="pd_dataframe_asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        print("✅ Fluent datasource and data asset created successfully!")

        # Define all expectations
        expectations = [
            # 1. Order ID must not be null
            gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID"),
            
            # 2. Order ID must be unique
            gx.expectations.ExpectColumnValuesToBeUnique(column="Order ID"),
            
            # 3. Qty must be non-negative
            gx.expectations.ExpectColumnValuesToBeBetween(column="Qty", min_value=0),
            
            # 4. Amount must be non-negative
            gx.expectations.ExpectColumnValuesToBeBetween(column="Amount", min_value=0),
            
            # 5. Status must be in allowed values
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="Status",
                value_set=['Cancelled', 'Shipped - Delivered to Buyer', 'Shipped', 'Shipped - Returned to Seller', 'Shipped - Rejected by Buyer', 'Shipped - Lost in Transit', 'Shipped - Out for Delivery', 'Shipped - Returning to Seller', 'Shipped - Picked Up', 'Pending', 'Pending - Waiting for Pick Up', 'Shipped - Damaged', 'Shipping']
            )
        ]

        print("✅ All expectations defined")

        # Run validation for each expectation
        validation_results = []
        for i, expectation in enumerate(expectations, 1):
            result = batch.validate(expectation)
            validation_results.append(result)
            print(f"✅ Validation {i}/{len(expectations)} completed: {expectation.__class__.__name__}")

        print("🎉 All Great Expectations validations completed successfully!")

        # Return the results for further analysis
        return {
            'results': validation_results,
            'expectations': expectations,
            'success': all(result.success for result in validation_results)
        }
        
    except Exception as e:
        print(f"❌ Great Expectations error: {e}")
        import traceback
        traceback.print_exc()
        return None

# Pydantic validation fonksiyonu (mevcut - çalışan versiyon)
def validate_with_pydantic(df):
    """Run Pydantic row-level validation with CSV-specific column mapping"""
    try:
        from pydantic import BaseModel, field_validator
        from datetime import datetime
        
        print("🔍 Running Pydantic row-level validation...")
        
        # Debug: Show unique values for critical columns to understand data
        print("📊 Data sample for validation columns:")
        critical_columns = ['currency', 'ship-country']
        for col in critical_columns:
            if col in df.columns:
                unique_vals = df[col].dropna().unique()
                print(f"  {col}: {list(unique_vals[:5])}... (total: {len(unique_vals)})")
        
        class AmazonOrder(BaseModel):
            order_id: str
            qty: int
            amount: float
            currency: str
            ship_country: str
            date: str
            
            @field_validator('order_id')
            def order_id_not_empty(cls, v):
                if not v or str(v).strip() == "" or str(v).lower() in ['nan', 'null', 'none']:
                    raise ValueError('Order ID cannot be empty')
                return str(v).strip()
            
            @field_validator('qty')
            def qty_non_negative(cls, v):
                # Handle NaN, None, and empty values
                if pd.isna(v) or v is None:
                    return 0
                try:
                    qty_val = int(float(v))  # Handle float strings
                    if qty_val < 0:
                        raise ValueError('Quantity must be ≥ 0')
                    return qty_val
                except (ValueError, TypeError):
                    raise ValueError('Quantity must be a valid number')
            
            @field_validator('amount')
            def amount_non_negative(cls, v):
                # Handle NaN, None, and empty values
                if pd.isna(v) or v is None:
                    return 0.0
                try:
                    amount_val = float(v)
                    if amount_val < 0:
                        raise ValueError('Amount must be ≥ 0')
                    return round(amount_val, 2)
                except (ValueError, TypeError):
                    raise ValueError('Amount must be a valid number')
            
            @field_validator('currency')
            def currency_must_be_inr(cls, v):
                # Handle NaN, None, and empty values
                if pd.isna(v) or v is None or not str(v).strip():
                    return "INR"  # Default value for empty/missing
                
                currency_val = str(v).strip().upper()
                # Check for common INR representations
                if currency_val in ['INR', '₹', 'RS', 'RUPEES', 'INR ']:
                    return "INR"
                else:
                    raise ValueError(f'Currency must be INR, got "{v}"')
            
            @field_validator('ship_country')
            def country_must_be_india(cls, v):
                # Handle NaN, None, and empty values
                if pd.isna(v) or v is None or not str(v).strip():
                    return "IN"  # Default value for empty/missing
                
                country_val = str(v).strip().upper()
                # Check for common India representations
                if country_val in ['IN', 'INDIA', 'IN ', 'IND', 'INDI']:
                    return "IN"
                else:
                    raise ValueError(f'Ship country must be IN, got "{v}"')
            
            @field_validator('date')
            def validate_date_format(cls, v):
                # Handle NaN, None, and empty values
                if pd.isna(v) or v is None or not str(v).strip():
                    raise ValueError('Date cannot be empty')
                
                date_str = str(v).strip()
                
                # Try multiple date formats that match your CSV (05-25-22)
                date_formats = [
                    "%m-%d-%y",  # 05-25-22 (your actual format)
                    "%m-%d-%Y",  # 05-25-2022
                    "%Y-%m-%d",  # 2022-05-25
                    "%d-%m-%Y",  # 25-05-2022
                    "%m/%d/%Y",  # 05/25/2022
                    "%m/%d/%y",  # 05/25/22
                    "%d-%m-%y",  # 25-05-22
                    "%d/%m/%Y",  # 25/05/2022
                    "%d/%m/%y"   # 25/05/22
                ]
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        # Return in consistent format
                        return parsed_date.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
                
                # If we get here, no format matched
                raise ValueError(f'Invalid date format: "{date_str}". Expected formats like MM-DD-YY (05-25-22)')
        
        # Validate each row
        valid_rows = []
        invalid_rows = []
        
        total_rows = len(df)
        print(f"🔄 Validating {total_rows} rows...")
        
        for index, row in df.iterrows():
            if index % 500 == 0:  # Progress indicator
                print(f"   Progress: {index}/{total_rows} rows...")
                
            try:
                # Direct column mapping from CSV header
                row_data = {
                    'order_id': row['Order ID'],
                    'qty': row['Qty'],
                    'amount': row['Amount'],
                    'currency': row['currency'],
                    'ship_country': row['ship-country'],
                    'date': row['Date']
                }
                
                # Validate the row using Pydantic
                order = AmazonOrder(**row_data)
                valid_rows.append(row.to_dict())
                
            except Exception as e:
                invalid_row = row.to_dict()
                invalid_row['validation_error'] = str(e)
                invalid_row['row_index'] = index
                invalid_rows.append(invalid_row)
                
                # Show first 5 errors for debugging
                if len(invalid_rows) <= 5:
                    error_msg = str(e).split('\n')[0]  # Only show first line of error
                    print(f"❌ Row {index} failed: {error_msg}")
        
        print(f"✅ Pydantic validation completed: {len(valid_rows)} valid, {len(invalid_rows)} invalid")
        
        # Show detailed error summary
        if invalid_rows:
            error_summary = {}
            for error in invalid_rows:
                error_msg = error['validation_error']
                # Extract the main error type (first line before any details)
                error_type = error_msg.split(':')[0] if ':' in error_msg else error_msg
                error_type = error_type.split('\n')[0]  # Take only first line
                error_summary[error_type] = error_summary.get(error_type, 0) + 1
            
            print("\n📊 Pydantic Error Summary:")
            for error_type, count in error_summary.items():
                percentage = (count / total_rows) * 100
                print(f"  - {error_type}: {count} rows ({percentage:.1f}%)")
        
        return {
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'total_rows': total_rows,
            'valid_count': len(valid_rows),
            'invalid_count': len(invalid_rows)
        }
        
    except Exception as e:
        print(f"❌ Pydantic validation failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_csv_files(pydantic_results):
    """Create CSV files from validation results"""
    try:
        if pydantic_results and pydantic_results['valid_rows']:
            valid_df = pd.DataFrame(pydantic_results['valid_rows'])
            # Remove the validation_error column if it exists
            if 'validation_error' in valid_df.columns:
                valid_df = valid_df.drop(columns=['validation_error'])
            valid_df.to_csv('valid_rows.csv', index=False)
            print(f"✅ Created valid_rows.csv with {len(valid_df)} rows")
        else:
            # Create empty valid_rows.csv if no valid rows
            pd.DataFrame().to_csv('valid_rows.csv', index=False)
            print("✅ Created empty valid_rows.csv")
        
        if pydantic_results and pydantic_results['invalid_rows']:
            invalid_df = pd.DataFrame(pydantic_results['invalid_rows'])
            invalid_df.to_csv('invalid_rows.csv', index=False)
            print(f"✅ Created invalid_rows.csv with {len(invalid_df)} rows")
            
            # Show sample of invalid rows for debugging
            print("📋 Sample of invalid rows:")
            for i, row in enumerate(invalid_df.head(3).to_dict('records')):
                print(f"  Row {i+1}: {row.get('validation_error', 'Unknown error')}")
        else:
            # Create empty invalid_rows.csv if no invalid rows
            pd.DataFrame().to_csv('invalid_rows.csv', index=False)
            print("✅ Created empty invalid_rows.csv")
            
        return True
    except Exception as e:
        print(f"❌ Error creating CSV files: {e}")
        return False

def main():
    """Main validation pipeline"""
    print("🚀 Starting Data Quality Validation Pipeline...")
    
    # Environment configuration
    environment = os.getenv('ENVIRONMENT', 'local')
    slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
    
    print(f"🏷️  Environment: {environment}")
    print(f"📱 Slack Enabled: {should_send_slack_notification()}")
    
    try:
        # Load data
        df = pd.read_csv('data/amazon_orders_sample.csv')
        print(f"📊 Loaded dataset: {len(df)} rows")
        
        # Debug dataframe structure
        debug_dataframe(df)
        
        validation_results = {
            'ge_success': False,
            'pydantic_success': False,
            'failed_expectations': 0,
            'invalid_rows': 0
        }
        
        # Run Great Expectations validation
        ge_results = validate_with_great_expectations(df)
        if ge_results:
            validation_results['ge_success'] = ge_results['success']
            validation_results['failed_expectations'] = len([
                r for r in ge_results['results'] if not r.success
            ])
            
            # Quick validation results analysis for GE
            total_expectations = len(ge_results['results'])
            successful = sum(1 for result in ge_results['results'] if result.success)
            failed = total_expectations - successful
            overall_ge_success = successful == total_expectations

            print(f"\n📊 Great Expectations Validation Summary")
            print("=" * 40)
            print(f"Overall Status: {'✅ PASSED' if overall_ge_success else '❌ FAILED'}")
            print(f"Successful: {successful}/{total_expectations}")

            if not overall_ge_success:
                print("\n❌ Failed expectations:")
                for i, result in enumerate(ge_results['results']):
                    if not result.success:
                        exp_type = ge_results['expectations'][i].__class__.__name__
                        print(f"  - {exp_type}")

            # Detailed analysis (only if validation failed)
            if not overall_ge_success:
                print("\n🔍 Detailed failure analysis:")
                for i, result in enumerate(ge_results['results']):
                    if not result.success:
                        exp_type = ge_results['expectations'][i].__class__.__name__
                        print(f"\n{exp_type}:")
                        # Check for unexpected values
                        if hasattr(result, 'result') and hasattr(result.result, 'get'):
                            unexpected = result.result.get('partial_unexpected_list', [])
                            if unexpected:
                                print(f"  Unexpected values: {unexpected[:3]}")
        else:
            print("❌ Great Expectations validation returned no results")
        
        print("\n" + "="*50)
        
        # Run Pydantic validation
        pydantic_results = validate_with_pydantic(df)
        if pydantic_results:
            validation_results['pydantic_success'] = pydantic_results['invalid_count'] == 0
            validation_results['invalid_rows'] = pydantic_results['invalid_count']
            
            # Create CSV files
            print("\n💾 Creating CSV files...")
            csv_created = create_csv_files(pydantic_results)
            if not csv_created:
                print("❌ Failed to create CSV files")
        else:
            print("❌ Pydantic validation returned no results")
        
        # Print overall summary
        print("\n📊 OVERALL VALIDATION SUMMARY")
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
        
        # Send notification if failed (environment-aware)
        if not overall_success:
            print("\n🔔 Handling notification...")
            notification_sent = send_slack_alert(slack_webhook, validation_results)
            
            if notification_sent:
                print("✅ Notification handled appropriately for environment")
            else:
                print("ℹ️  Notification skipped (environment configuration)")
        
        # Exit with appropriate code (CRITICAL for GitHub Actions)
        if overall_success:
            print("🎉 All validations passed!")
            sys.exit(0)
        else:
            print("❌ Validation failed - exiting with code 1")
            sys.exit(1)
            
    except Exception as e:
        print(f"💥 Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        
        # Create empty CSV files even on error
        try:
            pd.DataFrame().to_csv('valid_rows.csv', index=False)
            pd.DataFrame().to_csv('invalid_rows.csv', index=False)
            print("✅ Created empty CSV files after error")
        except:
            print("❌ Could not create CSV files after error")
        
        sys.exit(1)

if __name__ == "__main__":
    main()