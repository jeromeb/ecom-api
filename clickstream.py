import boto3
import random
import json
from datetime import datetime, timedelta
import ecom
ecommerce_db = ecom.Database(secret="dev/demodb")

ecommerce_db.connect()
get_products = ecommerce_db.execute_query_json("SELECT product_id,CAST(price AS DECIMAL(9,2)) AS price,category_id FROM Products");
ecommerce_db.close()

# print(get_products)
# seconds_in_a_year = 86400 * 365
# exit()


# Configuration
STREAM_NAME = "wf-demo-dp-clickstream"
NUM_RECORDS = 1000
region_name = "eu-west-1"

# Initialize the Kinesis client
session = boto3.session.Session(profile_name='data')
kinesis = session.client(
    service_name='kinesis',
    region_name=region_name
)

event_types = ['page_view', 'product_view', 'add_to_cart', 'checkout_start']
devices = ['desktop', 'mobile', 'tablet']

products = json.loads(get_products)

def generate_record(user_id):
    event_type = random.choice(event_types)
    device = random.choice(devices)
    session_id = f"session{user_id}"
    product = random.choice(products)
    seconds_in_a_year = 86400 * 365
    random_time = datetime.now() - timedelta(seconds=random.randint(0, seconds_in_a_year))
    record = {
        'user_id': user_id,
        'event_time': (datetime.now() - timedelta(seconds=random.randint(0, seconds_in_a_year))).strftime('%Y-%m-%d %H:%M:%S'),
        # 'event_time' : int(random_time.timestamp()),
        'event_type': event_type,
        'device': device,
        'session_id': session_id
    }

    if event_type in ['product_view', 'add_to_cart']:
        record.update({
            'product_id': product['product_id'],
            'category_id': product['category_id'],
            'price': product['price']
        })

    return record

def main():
    for _ in range(NUM_RECORDS):
        user_id = random.randint(1, 400)  # Assuming 200 users as per provided data
        record = generate_record(user_id)
        print(f"Pushing record: {record}")
        kinesis.put_record(StreamName=STREAM_NAME, Data=json.dumps(record), PartitionKey=str(user_id))

if __name__ == "__main__":
    main()