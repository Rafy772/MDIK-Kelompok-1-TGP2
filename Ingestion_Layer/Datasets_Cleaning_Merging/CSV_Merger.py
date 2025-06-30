import json
import pandas as pd
from datetime import datetime
import re

def load_json_file(file_path, is_structured=True):
    """Load JSON data from file—handles structured (array or single object)
    and unstructured (JSON Lines) formats."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if is_structured:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    # Fallback to line-by-line if there’s extra data
                    f.seek(0)
                    return _load_json_lines(f)
                if isinstance(data, dict):
                    return [data]
                return data
            else:
                return _load_json_lines(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return []

def _load_json_lines(file_obj):
    """Helper to parse a file of one JSON object per line."""
    data = []
    for line_num, line in enumerate(file_obj, 1):
        line = line.strip()
        if not line:
            continue
        try:
            data.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
    return data

def safe_get_value(record, key, default=None):
    """Safely get value from record with fallback options"""
    if isinstance(record, dict):
        return record.get(key, default)
    return default

def extract_nested_value(record, keys):
    """Extract value from nested structure using multiple possible keys"""
    if not isinstance(record, dict):
        return None
    
    for key in keys:
        if key in record:
            value = record[key]
            if value is not None and value != '':
                return value
    return None

def process_sales_data(sales_data):
    """Process sales/invoice data (structured)"""
    processed = []
    for record in sales_data:
        try:
            # Parse date with multiple format attempts
            date_str = safe_get_value(record, 'InvoiceDate', '')
            parsed_date = parse_flexible_date(date_str)
            
            processed_record = {
                'product_id': safe_get_value(record, 'StockCode', ''),
                'record_type': 'sales',
                'transaction_id': safe_get_value(record, 'InvoiceNo', ''),
                'product_title': safe_get_value(record, 'Description', ''),
                'customer_id': safe_get_value(record, 'CustomerID', ''),
                'country': safe_get_value(record, 'Country', ''),
                'quantity_sold': safe_float_conversion(safe_get_value(record, 'Quantity')),
                'unit_price': safe_float_conversion(safe_get_value(record, 'UnitPrice')),
                'total_value': calculate_total_value(record),
                'date': parsed_date.strftime('%Y-%m-%d') if parsed_date else '',
                'year_month': parsed_date.strftime('%Y-%m') if parsed_date else '',
                'rating': None,
                'average_rating': None,
                'review_sentiment': None,
                'verified_purchase': None,
                'review_count': None,
                'category': extract_category_from_description(safe_get_value(record, 'Description', ''))
            }
            processed.append(processed_record)
        except Exception as e:
            print(f"Error processing sales record: {e}")
            continue
    return processed

def process_review_data(review_data):
    """Process review data (unstructured)"""
    processed = []
    for record in review_data:
        try:
            # Handle various possible field names for unstructured data
            rating = extract_nested_value(record, ['rating', 'score', 'stars', 'rate'])
            title = extract_nested_value(record, ['title', 'review_title', 'summary'])
            text = extract_nested_value(record, ['text', 'review_text', 'comment', 'review'])
            user_id = extract_nested_value(record, ['user_id', 'customer_id', 'reviewer_id', 'user'])
            asin = extract_nested_value(record, ['asin', 'product_id', 'item_id'])
            timestamp = extract_nested_value(record, ['timestamp', 'date', 'review_date', 'time'])
            verified = extract_nested_value(record, ['verified_purchase', 'verified', 'confirmed_purchase'])
            
            # Parse timestamp flexibly
            parsed_date = parse_flexible_timestamp(timestamp)
            
            # Convert rating to float
            rating_float = safe_float_conversion(rating)
            
            # Determine sentiment
            sentiment = determine_sentiment(rating_float, text)
            
            processed_record = {
                'product_id': str(asin) if asin else '',
                'record_type': 'review',
                'transaction_id': f"review_{user_id}_{timestamp}" if user_id and timestamp else f"review_{len(processed)}",
                'product_title': str(title) if title else '',
                'customer_id': str(user_id) if user_id else '',
                'country': None,
                'quantity_sold': None,
                'unit_price': None,
                'total_value': None,
                'date': parsed_date.strftime('%Y-%m-%d') if parsed_date else '',
                'year_month': parsed_date.strftime('%Y-%m') if parsed_date else '',
                'rating': rating_float,
                'average_rating': None,
                'review_sentiment': sentiment,
                'verified_purchase': bool(verified) if verified is not None else None,
                'review_count': None,
                'category': None
            }
            processed.append(processed_record)
        except Exception as e:
            print(f"Error processing review record: {e}")
            continue
    return processed

def process_catalog_data(catalog_data):
    """Process product catalog data (unstructured)"""
    processed = []
    for record in catalog_data:
        try:
            # Handle various possible field names
            title = extract_nested_value(record, ['title', 'name', 'product_name', 'item_name'])
            asin = extract_nested_value(record, ['asin', 'product_id', 'item_id', 'id'])
            parent_asin = extract_nested_value(record, ['parent_asin', 'parent_id', 'main_asin'])
            avg_rating = extract_nested_value(record, ['average_rating', 'avg_rating', 'rating', 'overall_rating'])
            rating_count = extract_nested_value(record, ['rating_number', 'rating_count', 'review_count', 'num_reviews'])
            category = extract_nested_value(record, ['main_category', 'category', 'department', 'section'])
            price = extract_nested_value(record, ['price', 'unit_price', 'cost', 'amount'])
            
            # Convert to appropriate types
            avg_rating_float = safe_float_conversion(avg_rating)
            rating_count_int = safe_int_conversion(rating_count)
            price_float = extract_flexible_price(price)
            
            product_id = str(parent_asin) if parent_asin else str(asin) if asin else ''
            
            processed_record = {
                'product_id': product_id,
                'record_type': 'catalog',
                'transaction_id': f"catalog_{product_id}",
                'product_title': str(title) if title else '',
                'customer_id': None,
                'country': None,
                'quantity_sold': None,
                'unit_price': price_float,
                'total_value': None,
                'date': '',
                'year_month': '',
                'rating': None,
                'average_rating': avg_rating_float,
                'review_sentiment': None,
                'verified_purchase': None,
                'review_count': rating_count_int,
                'category': str(category) if category else ''
            }
            processed.append(processed_record)
        except Exception as e:
            print(f"Error processing catalog record: {e}")
            continue
    return processed

def extract_category_from_description(description):
    """Extract category from product description using keywords"""
    if not description:
        return None
    
    description_upper = description.upper()
    categories = {
        'HOME_DECOR': ['HEART', 'HOLDER', 'DECORATION', 'ORNAMENT'],
        'KITCHEN': ['KITCHEN', 'COOKING', 'UTENSIL'],
        'GARDEN': ['GARDEN', 'OUTDOOR', 'PLANT'],
        'GIFT': ['GIFT', 'PRESENT'],
        'LIGHTING': ['LIGHT', 'LAMP', 'CANDLE']
    }
    
    for category, keywords in categories.items():
        if any(keyword in description_upper for keyword in keywords):
            return category
    return 'OTHER'

def safe_float_conversion(value):
    """Safely convert value to float"""
    if value is None or value == '':
        return None
    try:
        return float(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return None

def safe_int_conversion(value):
    """Safely convert value to int"""
    if value is None or value == '':
        return None
    try:
        return int(float(str(value).replace(',', '')))
    except (ValueError, TypeError):
        return None

def calculate_total_value(record):
    """Calculate total value from quantity and unit price"""
    quantity = safe_float_conversion(safe_get_value(record, 'Quantity'))
    unit_price = safe_float_conversion(safe_get_value(record, 'UnitPrice'))
    if quantity is not None and unit_price is not None:
        return quantity * unit_price
    return None

def parse_flexible_date(date_str):
    """Parse date string with multiple format attempts"""
    if not date_str:
        return None
    
    formats = [
        '%m/%d/%Y %H:%M',
        '%d/%m/%Y %H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except (ValueError, TypeError):
            continue
    return None

def parse_flexible_timestamp(timestamp):
    """Parse timestamp with multiple format attempts"""
    if timestamp is None:
        return None
    
    try:
        # Try as Unix timestamp (seconds)
        if isinstance(timestamp, (int, float)):
            if timestamp > 1e10:  # Milliseconds
                return datetime.fromtimestamp(timestamp / 1000)
            else:  # Seconds
                return datetime.fromtimestamp(timestamp)
        
        # Try as string
        timestamp_str = str(timestamp)
        if timestamp_str.isdigit():
            ts_num = int(timestamp_str)
            if ts_num > 1e10:  # Milliseconds
                return datetime.fromtimestamp(ts_num / 1000)
            else:  # Seconds
                return datetime.fromtimestamp(ts_num)
        
        # Try as date string
        return parse_flexible_date(timestamp_str)
    except (ValueError, TypeError, OSError):
        return None

def determine_sentiment(rating, text=None):
    """Determine sentiment from rating and optionally text"""
    if rating is not None:
        if rating >= 4:
            return 'positive'
        elif rating >= 3:
            return 'neutral'
        else:
            return 'negative'
    
    # If no rating, try basic text analysis
    if text and isinstance(text, str):
        text_lower = text.lower()
        positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'perfect', 'wonderful']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'horrible', 'useless', 'waste']
        
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        else:
            return 'neutral'
    
    return None

def extract_flexible_price(price):
    """Extract price from various formats"""
    if price is None:
        return None
    
    try:
        # If already a number
        if isinstance(price, (int, float)):
            return float(price)
        
        # If string, extract numeric value
        price_str = str(price)
        # Remove currency symbols and extract numbers
        import re
        price_match = re.search(r'[\d,]+\.?\d*', price_str.replace(',', ''))
        if price_match:
            return float(price_match.group().replace(',', ''))
    except (ValueError, TypeError):
        pass
    
    return None

def create_unified_csv(sales_file, review_file, catalog_file, output_file):
    """Main function to create unified CSV"""
    print("Loading JSON files...")
    
    # Load data - sales is structured, others are unstructured
    sales_data = load_json_file(sales_file, is_structured=True)
    review_data = load_json_file(review_file, is_structured=False)
    catalog_data = load_json_file(catalog_file, is_structured=False)
    
    print(f"Loaded {len(sales_data)} sales records")
    print(f"Loaded {len(review_data)} review records") 
    print(f"Loaded {len(catalog_data)} catalog records")
    
    # Show sample records to understand structure
    print("\n--- Sample Data Structure ---")
    if sales_data:
        print("Sales sample:", list(sales_data[0].keys())[:5])
    if review_data:
        print("Review sample:", list(review_data[0].keys())[:5])
    if catalog_data:
        print("Catalog sample:", list(catalog_data[0].keys())[:5])
    
    # Process each dataset
    print("\nProcessing data...")
    processed_sales = process_sales_data(sales_data)
    processed_reviews = process_review_data(review_data)
    processed_catalog = process_catalog_data(catalog_data)
    
    print(f"Processed {len(processed_sales)} sales records")
    print(f"Processed {len(processed_reviews)} review records")
    print(f"Processed {len(processed_catalog)} catalog records")
    
    # Combine all data
    all_data = processed_sales + processed_reviews + processed_catalog
    
    if not all_data:
        print("No data processed successfully!")
        return None
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Add calculated fields for correlation analysis
    df['price_rating_ratio'] = None
    mask = (df['unit_price'].notna()) & (df['average_rating'].notna()) & (df['average_rating'] > 0)
    df.loc[mask, 'price_rating_ratio'] = df.loc[mask, 'unit_price'] / df.loc[mask, 'average_rating']
    
    # Sort by product_id and record_type for better organization
    df = df.sort_values(['product_id', 'record_type'])
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    
    print(f"\nUnified CSV created: {output_file}")
    print(f"Total records: {len(df)}")
    print(f"Unique products: {df['product_id'].nunique()}")
    print("\nRecord type distribution:")
    print(df['record_type'].value_counts())
    
    # Show data quality summary
    print("\n--- Data Quality Summary ---")
    print(f"Records with price data: {df['unit_price'].notna().sum()}")
    print(f"Records with rating data: {df['rating'].notna().sum()}")
    print(f"Records with average rating: {df['average_rating'].notna().sum()}")
    print(f"Records with price-rating ratio: {df['price_rating_ratio'].notna().sum()}")
    
    # Show sample for price vs rating analysis
    print("\nSample data for Price vs Rating analysis:")
    price_rating_sample = df[df['price_rating_ratio'].notna()][['product_id', 'product_title', 'unit_price', 'average_rating', 'price_rating_ratio']].head()
    if not price_rating_sample.empty:
        print(price_rating_sample)
    else:
        print("No records found with both price and rating data for correlation analysis")
    
    return df

# Example usage
if __name__ == "__main__":
    # Replace these with your actual file paths
    sales_file = "kaggle.json"
    review_file = "review_Subscription_Boxes.json" 
    catalog_file = "meta_Subscription_Boxes.json"
    output_file = "unified_data.csv"
    
    # Create unified CSV
    df = create_unified_csv(sales_file, review_file, catalog_file, output_file)
    
    print(f"\nColumns in unified CSV: {list(df.columns)}")