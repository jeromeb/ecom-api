import ecom
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
app = Flask(__name__)
CORS(app)

# Check if environment variables are set
css_secret = os.environ.get('DB_CSS_SECRET')
ecom_secret = os.environ.get('DB_ECOM_SECRET')
scms_secret = os.environ.get('DB_SCMS_SECRET')

# Initialize database objects based on whether secrets are set
start_app=False
if css_secret:
    css_db = ecom.Database(secret=css_secret)
    print("CSS database initialized.")
    start_app=True
else:
    css_db = None
    print("CSS secret not found. CSS database not initialized.")

if ecom_secret:
    ecommerce_db = ecom.Database(secret=ecom_secret)
    print("Ecommerce database initialized.")
    start_app=True
else:
    ecommerce_db = None
    print("Ecommerce secret not found. Ecommerce database not initialized.")

if scms_secret:
    scms_db = ecom.Database(secret=scms_secret)
    print("SCMS database initialized.")
    start_app=True
else:
    scms_db = None
    print("SCMS secret not found. SCMS database not initialized.")

if not start_app:
    print("No database secrets found. Exiting.")
    exit(1)

@app.route('/')
def home():
   return jsonify({'message': 'Welcome to the Ecommerce API!'})

@app.route('/orders', methods=['POST'])
def create_order():
    #get parameter from json 
    # if not order_cnt or not order_start_date or not order_end_date:
    #     return jsonify({"error": "Missing parameters"}), 400    
    order_cnt = request.json['quantity']
    order_start_date = request.json['start_date']
    order_end_date = request.json['end_date']
    # Convert start and end dates to datetime objects
    start_time = datetime.now()
    orders = ecom.Orders(ecommerce_db)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(orders.generate_order, order_start_date, order_end_date) for _ in range(int(order_cnt))]
        result = []
        for future in futures:
            result.append(future.result())
            pass
    end_time = datetime.now()
    # count the items in the result list
    # calculate difference between start and end time in seconds 
    diff = end_time - start_time

    response = jsonify({'message': f'{order_cnt} orders created successfully!','processing_time_sec':f'{diff.total_seconds()}'})
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

# create a new route to fetch an order by ID 
@app.route('/orders/<int:order_id>', methods=['GET'])
def get_order(order_id):
    orders = ecom.Orders(ecommerce_db)
    response = jsonify(orders.get_orders(None,order_id=order_id))
    return response

@app.route('/orders', methods=['GET'])
def get_orders():
    limit = request.args.get('limit', type=int)
    order_field = request.args.get('order_by', type=str)
    orders = ecom.Orders(ecommerce_db)
    response = jsonify(orders.get_orders(limit,order_field))
    return response

# create a new route to update the order status based on the order id
@app.route('/orders/<int:order_id>/status', methods=['PUT'])
def update_order_status(order_id):
    status = request.json['status']
    orders = ecom.Orders(ecommerce_db)
    orders.update_order_status(order_id, status)
    return jsonify({'message': f'Order status updated successfully!'})

@app.route('/users', methods=['POST'])
def create_user():
    user_cnt = request.json['quantity']
    users = ecom.Users(ecommerce_db)
    users.insert_random_user(int(user_cnt))
    return jsonify({'message': f'{user_cnt} users created successfully!'})

@app.route('/inventory', methods=['POST'])
def create_inventory():
    inv_count = request.json['quantity']
    products = ecom.Products(ecommerce_db)
    supply_chain = ecom.SupplyChain(scms_db)
    product_ids = products.get_random_product(inv_count)
    print(product_ids)
    supply_chain.create_inventory(product_ids)
    return jsonify({'message': f'{inv_count} inventories created successfully!'})

@app.route('/inventory', methods=['GET'])
def get_inventory():
    limit = request.args.get('limit', type=int)
    supply_chain = ecom.SupplyChain(scms_db)
    response = jsonify(supply_chain.get_inventory(limit))
    return response

@app.route('/users', methods=['GET'])
def get_user():
    limit = request.args.get('limit', type=int)
    print(limit)
    users = ecom.Users(ecommerce_db)
    response = jsonify(users.get_user(limit))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

#add GET product route
@app.route('/products', methods=['GET'])
def get_product():
    limit = request.args.get('limit', type=int)
    print(limit)
    products = ecom.Products(ecommerce_db)
    return jsonify(products.get_product(limit))

#add POST reviews route 
@app.route('/reviews', methods=['POST'])
def create_review():
    review_cnt = request.form['number_reviews']
    reviews = ecom.Reviews(ecommerce_db)
    reviews.generate_reviews_for_random_orders(int(review_cnt))
    return jsonify({'message': f'{review_cnt} reviews created successfully!'})

#add GET categories route 
@app.route('/categories', methods=['GET'])
def get_category():
    products = ecom.Products(ecommerce_db)
    return jsonify(products.get_categories())

#add GET reviews route
@app.route('/reviews', methods=['GET'])
def get_review():
    limit = request.args.get('limit', type=int)
    print(limit)
    reviews = ecom.Reviews(ecommerce_db)
    return jsonify(reviews.get_review(limit))

#add GET tickets route
@app.route('/support/tickets', methods=['GET'])
def get_tickets():
    limit = request.args.get('limit', type=int)
    cs = ecom.CustomerSupport(css_db)
    return jsonify(cs.get_tickets(limit))

@app.route('/support/resolve', methods=['POST'])
def resolve_tickets():
    ticket_cnt = request.json['number_tickets']
    cs = ecom.CustomerSupport(css_db)
    cs.resolve_support_tickets(ticket_cnt)
    return jsonify({'message': f'{ticket_cnt} support tickets resolved!'})


@app.route('/support/ticket', methods=['POST'])
def create_support_ticket():
    # ticket_cnt = request.form['number_tickets']
    ticket_cnt = request.json['number_tickets']
    users = ecom.Users(ecommerce_db)
    products = ecom.Products(ecommerce_db)
    cs = ecom.CustomerSupport(css_db)
    user_ids = users.get_random_user(ticket_cnt)
    product_ids = products.get_random_product(ticket_cnt)
    cs.insert_random_support_tickets(int(ticket_cnt),user_ids,product_ids)
    return jsonify({'message': f'{ticket_cnt} support tickets created successfully!'})


if __name__ == '__main__':
   app.run(debug=os.environ['DEBUG'],host='0.0.0.0', port=3000)
   
