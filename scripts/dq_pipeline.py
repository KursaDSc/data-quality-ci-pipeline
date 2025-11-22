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

def debug_dataframe(df):
    """Debug dataframe structure"""
    print("🔍 DEBUG: DataFrame Info")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"First 2 rows:")
    print(df.head(2))
    print("=" * 50)

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

def validate_with_pydantic(df):
    """Run Pydantic row-level validation"""
    try:
        from pydantic import BaseModel, field_validator
        from datetime import datetime
        
        print("🔍 Running Pydantic row-level validation...")
        
        # Debug: Show available columns for mapping
        print(f"📋 Available columns: {list(df.columns)}")
        
        class AmazonOrder(BaseModel):
            # Use flexible field mapping based on actual CSV columns
            order_id: str
            qty: int
            amount: float
            currency: str = "INR"  # Default value
            ship_country: str = "IN"  # Default value
            date: str
            
            @field_validator('order_id')
            def order_id_not_empty(cls, v):
                if not v or str(v).strip() == "":
                    raise ValueError('Order ID cannot be empty')
                return str(v)
            
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
                # Some rows might have different currencies, we'll allow INR or empty
                if v and v != "INR":
                    raise ValueError(f'Currency must be INR, got {v}')
                return v
            
            @field_validator('ship_country')
            def country_must_be_india(cls, v):
                # Some rows might have different countries, we'll allow IN or empty
                if v and v != "IN":
                    raise ValueError(f'Ship country must be IN, got {v}')
                return v
            
            @field_validator('date')
            def validate_date_format(cls, v):
                try:
                    # Try multiple date formats including 2-digit year
                    date_formats = [
                        "%m-%d-%y",  # 05-25-22
                        "%m-%d-%Y",  # 05-25-2022
                        "%Y-%m-%d",  # 2022-05-25
                        "%d-%m-%Y",  # 25-05-2022
                        "%m/%d/%Y",  # 05/25/2022
                        "%m/%d/%y"   # 05/25/22
                    ]
                    
                    for fmt in date_formats:
                        try:
                            datetime.strptime(str(v), fmt)
                            return v
                        except ValueError:
                            continue
                    
                    # If none of the formats work, check if it's already a datetime object
                    if isinstance(v, datetime):
                        return v.strftime("%m-%d-%Y")
                    
                    raise ValueError(f'Invalid date format: {v}. Expected formats: MM-DD-YY or similar')
                except Exception as e:
                    raise ValueError(f'Date validation error: {e}')
        
        # Validate each row
        valid_rows = []
        invalid_rows = []
        
        for index, row in df.iterrows():
            try:
                # Flexible column mapping with fallbacks
                row_data = {
                    'order_id': row.get('Order ID', ''),
                    'qty': row.get('Qty', 0),
                    'amount': row.get('Amount', 0.0),
                    'currency': row.get('currency', 'INR'),  # Default to INR
                    'ship_country': row.get('ship-country', 'IN'),  # Default to IN
                    'date': row.get('Date', '')
                }
                
                # Skip validation for completely empty rows
                if not row_data['order_id'] and row_data['qty'] == 0 and row_data['amount'] == 0.0:
                    print(f"⚠️  Skipping empty row {index}")
                    continue
                
                order = AmazonOrder(**row_data)
                valid_rows.append(row.to_dict())
                
            except Exception as e:
                invalid_row = row.to_dict()
                invalid_row['validation_error'] = str(e)
                invalid_row['row_index'] = index
                invalid_rows.append(invalid_row)
                print(f"❌ Row {index} failed: {e}")
        
        print(f"✅ Pydantic validation: {len(valid_rows)} valid, {len(invalid_rows)} invalid")
        
        return {
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'total_rows': len(df),
            'valid_count': len(valid_rows),
            'invalid_count': len(invalid_rows)
        }
        
    except Exception as e:
        print(f"❌ Pydantic error: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_csv_files(pydantic_results):
    """Create CSV files from validation results"""
    try:
        if pydantic_results and pydantic_results['valid_rows']:
            valid_df = pd.DataFrame(pydantic_results['valid_rows'])
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
        else:
            # Create empty invalid_rows.csv if no invalid rows
            pd.DataFrame().to_csv('invalid_rows.csv', index=False)
            print("✅ Created empty invalid_rows.csv")
            
        return True
    except Exception as e:
        print(f"❌ Error creating CSV files: {e}")
        return False

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
                                print(f"  Unexpected values: {unexpected[:3]}")  # Show first 3
        else:
            print("❌ Great Expectations validation returned no results")
        
        # Run Pydantic validation
        pydantic_results = validate_with_pydantic(df)
        if pydantic_results:
            validation_results['pydantic_success'] = pydantic_results['invalid_count'] == 0
            validation_results['invalid_rows'] = pydantic_results['invalid_count']
            
            # Create CSV files
            csv_created = create_csv_files(pydantic_results)
            if not csv_created:
                print("❌ Failed to create CSV files")
        
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