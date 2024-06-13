import json
import time
import mysql.connector
from mysql.connector import pooling
import random
import string
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError

class Database:
    def __init__(self, secret):
        self.config = json.loads(self._read_config(secret))
        self.db_config = {
            'user': self.config['username'],
            'password': self.config['password'],
            'host': self.config['host'],
            'database': self.config['dbname']
        }
        pool_name = "mypool"
        pool_size = 5
        print("creating MySQL Connection Pool")
        self.cnxpool = pooling.MySQLConnectionPool(pool_name=pool_name,
                                      pool_size=pool_size,
                                      **self.db_config)        
        print(self.cnxpool)

    def _read_config(self,secret_name,region_name="us-east-1"):

        # do a case to select options
        if secret_name == 'DatabaseEcomSecret50B0C388-opRRg2o7aaTF':
            return '{"dbClusterIdentifier":"ecom-cluster","password":"7hG6AO72OlbT2gi9^WzWtrcfGGlqtb","dbname":"ecomdb","engine":"mysql","port":3306,"host":"ecom-cluster.cluster-cvhirquebsma.us-east-1.rds.amazonaws.com","username":"ecomuser"}'
        elif secret_name == 'DatabaseCSSSecretD03DE3E4-Sk9q5RHn6DI9':
            return '{"dbClusterIdentifier":"css-cluster","password":"^L6b0m1Zbm^o,3lzXA,1zaZI-ICFd0","dbname":"cssdb","engine":"mysql","port":3306,"host":"css-cluster.cluster-cvhirquebsma.us-east-1.rds.amazonaws.com","username":"cssuser"}'
        elif secret_name == 'DatabaseSCMSSecret419E44BA-1RUpdMlySpJV':
            return '{"dbClusterIdentifier":"scms-cluster","password":"Xci-zgwTfvsw7YvaeUOtqGza-0nlbg","dbname":"scmsdb","engine":"mysql","port":3306,"host":"scms-cluster.cluster-cvhirquebsma.us-east-1.rds.amazonaws.com","username":"scmsuser"}'
        else:
            session = boto3.session.Session(profile_name='data')
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
            try:
                get_secret_value_response = client.get_secret_value(
                    SecretId=secret_name
                )
                # Decrypts secret using the associated KMS key.
                # print(get_secret_value_response['SecretString'])
                return get_secret_value_response['SecretString']            
            except ClientError as e:
                # For a list of exceptions thrown, see
                # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
                raise e

    def connect(self):
        try:
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                cursor = connection.cursor()
                # print("Connected to MySQL")
                return connection, cursor
        except mysql.connector.Error as e:
            print(f"Error: {e}")
            return None, None

    def execute_query_json(self, query):
        connection, cursor = self.connect()
        if connection and cursor:
            try:
                cursor.execute(query)
                # Perform your SQL operations here
                connection.commit()
                return _convert_to_json(self.cursor)
            except mysql.connector.Error as e:
                print(f"SQL Error: {e}")
            finally:
                cursor.close()
                connection.close()        
        
    def execute_query(self, query):
        connection, cursor = self.connect()
        if connection and cursor:
            try:
                cursor.execute(query)
                # Perform your SQL operations here
                connection.commit()
                return self.cursor.fetchall()
            except mysql.connector.Error as e:
                print(f"SQL Error: {e}")
            finally:
                cursor.close()
                connection.close()              

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Connection closed")

class Users:
    def __init__(self, db):
        self.db = db
        self.cnxpool = db.cnxpool

    def insert_random_user(self, num_users):
        user_records = []
        cnxpool = self.cnxpool
        # print(f"creating {num_users} new users")
        connection = cnxpool.get_connection()
        cursor = connection.cursor()

        try:

            for _ in range(num_users):
                password = self._generate_random_password()
                first_name = self._generate_random_name()
                last_name = self._generate_random_lastname()
                # print(f"{first_name}.{last_name} - {password}")
                username = self._generate_random_username(first_name, last_name)
                email = self._generate_random_email(username)
                # date_joined = datetime.now() - timedelta(days=random.randint(1, 365))
                end_date = datetime.now().date()
                start_date =  datetime(end_date.year , 1, 1).date()
                date_joined = _random_date(start_date, end_date) 
                is_admin = random.choice([True, False])

                user_records.append(
                    (username, email, password, first_name, last_name, date_joined, is_admin))

            query = "INSERT INTO Users (username, email, password, first_name, last_name, date_joined, is_admin) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            # values = (username, email, password, first_name, last_name, date_joined, is_admin)
            cursor.executemany(query, user_records)
            connection.commit()
            user_id = cursor.lastrowid
            print(user_id)
            self.insert_random_address(user_id)
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()            

    def insert_random_address(self, user_id):
        cnxpool = self.cnxpool
        print(f"adding address for user id {user_id}")
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
    
        try:
            street = self._generate_random_street()
            city = self._generate_random_city()
            country = self._generate_random_country()
            #escape single quote on street
            street = street.replace("'", "''")
            query = "INSERT INTO ShippingAddresses (user_id, street, city, state, country, postal_code) " \
                f"VALUES ({user_id}, '{street}','{city}', null, '{country}', null)"   
            print(query)
            cursor.execute(query)
            connection.commit()
        
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()


    def get_random_user(self, limit,user_identifier=None):

        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        dbname = 'ecomdb'
        user_ids =[]
        try:
            cursor.execute(f"SELECT min(user_id) AS first_user,max(user_id) AS last_user FROM {dbname}.Users")
            users = _convert_to_json(cursor)[0]
            for i in range(limit):
                user_ids.append(random.randint(users['first_user'], users['last_user']))
            return user_ids
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()  


    def get_user(self, limit,user_identifier=None):

        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
    
        try:
            if user_identifier is None:
                query = f"SELECT * FROM Users ORDER BY user_id DESC {_get_limit(limit)}"
            elif isinstance(user_identifier, int):
                query = "SELECT * FROM Users WHERE user_id = %s"
            elif isinstance(user_identifier, str)  and user_identifier != 'random':
                query = "SELECT * FROM Users WHERE username = %s"
            elif user_identifier == 'random':
                query = "SELECT * FROM Users ORDER BY RAND() LIMIT 1"

            if user_identifier is None or user_identifier == 'random':
                cursor.execute(query)
                return _convert_to_json(cursor)
            else:
                cursor.execute(query, (user_identifier,))
                return _convert_to_json(cursor.fetchone())
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()            

    def _generate_random_username(self, first_name, last_name):
        num = random.randint(100, 99999)
        return f"{first_name}{last_name}{num}"

    def _generate_random_email(self, username):
        domains = ['example.com', 'test.com', 'randommail.com', 'domain1.com', 'domain2.net',
                   'emailplace.org', 'yourmail.edu', 'mailnow.co', 'inboxmail.io', 'fastmail.tech',
                   'myemail.site', 'mailhere.biz', 'uniqueemail.store', 'greatmail.online', 'emailnow.info',
                   'bestemail.space', 'quickmail.xyz', 'topmail.store', 'mailinglist.pro', 'mailhub.live']
        return f"{username}@{random.choice(domains)}"

    def _generate_random_password(self):
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(12))

    def _generate_random_lastname(self):
        names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor',
                 'Anderson', 'Thomas', 'Jackson', 'White', 'Harris', 'Martin', 'Thompson', 'Garcia',  'Robinson', "Garcia",  "Rodriguez", "Lopez", "Hernandez",
                 "Gonzalez", "Perez", "Ramirez", "Torres", "Flores",
                 "Mendoza", "Diaz", "Castro", "Vargas", "Ortega",
                 "Vega", "Morales", "Soto", "Delgado", "Figueroa",
                 "Dupont", "Martin", "Dubois", "Laurent", "Lefebvre",
                 "Girard", "Moreau", "Simon", "Rousseau", "Petit",
                 "Blanc", "Mercier", "Faure", "Roux", "Gauthier",
                 "Leclerc", "Caron", "Mathieu", "Leroy", "Richard",
                 "Smith", "Johnson", "Brown", "Taylor", "Anderson",
                 "Wilson", "Martinez", "Harris", "Clark", "Lee",
                 "Hall", "Turner", "White", "King", "Lewis",
                 "Wright", "Green", "Adams", "Nelson", "Baker",    'Young', 'Allen', 'Mitchell', 'King', 'Lewis', 'Walker',
                 'Hall', 'Parker', 'Collins', 'Carter', 'Bell', 'Cook',
                 'Bennett', 'Hill', 'Harris', 'Allen', 'Reed', 'Scott',
                 'Morgan', 'Barnes', 'Howard', 'Ward', 'Cooper', 'Long',
                 'Ross', 'Hughes', 'Turner', 'Edwards', 'Wood', 'Brooks',
                 'Sullivan', 'Jenkins', 'Price', 'Stewart', 'Miller',
                 'Baker', 'Bailey', 'Cruz', 'Butler', 'Lopez', 'Powell',
                 'Cox', 'Ward', 'Gray', 'Adams', 'Perez', 'Foster',
                 'Gomez', 'Russell', 'Bryant', 'Myers', 'Sanders',
                 'Coleman', 'Jordan', 'Reyes', 'Diaz', 'Bryant', 'Griffin']
        return random.choice(names)

    def _generate_random_name(self):
        names = ['John', 'Jane', 'Michael', 'Emily', 'William', 'Emma', 'Oliver', 'Ava',
                 'Liam', 'Sophia', 'Noah', 'Olivia', 'Ethan', 'Isabella', 'James', 'Mia',
                 'Alexander', 'Charlotte', 'Benjamin', 'Amelia', 'Henry', 'Harper', 'Daniel', 'Evelyn',
                 'Matthew', 'Luna', 'David', 'Lily', 'Joseph', 'Grace', 'Samuel', 'Aria',
                 'Jackson', 'Scarlett', 'Sebastian', 'Chloe', 'Aiden', 'Zoe', 'Lucas', 'Riley',
                 'Carter', 'Layla', 'Jayden', 'Madison', 'Daniel', 'Elizabeth', 'Jack', 'Ella',
                 'Owen', 'Avery', 'Wyatt', 'Sofia', 'Luke', 'Camila', 'Gabriel', 'Aubrey',
                 'Anthony', 'Emily', 'Dylan', 'Abigail', 'Leo', 'Nora', 'Andrew', 'Scarlett',
                 'Lincoln', 'Hannah', 'Isaac', 'Addison', 'Christopher', 'Eleanor', 'Joshua', 'Victoria',
                 'Nathan', 'Grace', 'Eli', 'Ellie', 'Jonathan', 'Lillian', 'Jameson', 'Natalie',
                 'Caleb', 'Lily', 'Ryan', 'Zoe', 'Oliver', 'Audrey', 'Josiah', 'Stella',
                 'David', 'Leah', 'William', 'Lucy', 'Nolan', 'Bella', 'Evan', 'Nova',
                 'Seth', 'Zara', 'Asher', 'Skylar', 'Henry', 'Aurora', 'Charles', 'Emilia']
        return random.choice(names)

    def _generate_random_street(self):
            streets = [
                "Green St", "Oak Rd", "Maple Ave", "Pine Ln", "Elm St",
                "Cedar Blvd", "Cherry Cir", "Birch Pl", "Walnut Dr", "Willow Way",
                "Poplar St", "Ash Ave", "Beech Rd", "Holly Ln", "Fir St",
                "Spruce Blvd", "Chestnut Cir", "Alder Pl", "Magnolia Dr", "Hazel Way",
                "Redwood St", "Dogwood Ave", "Sycamore Rd", "Juniper Ln", "Cypress St",
                "Aspen Blvd", "Sequoia Cir", "Hemlock Pl", "Larch Dr", "Palm Way",
                "Laurel St", "Teak Ave", "Elder Rd", "Myrtle Ln", "Olive St",
                "Pear Blvd", "Peach Cir", "Plum Pl", "Fig Dr", "Lime Way",
                "Apple St", "Berry Ave", "Grape Rd", "Melon Ln", "Banana St",
                "Orange Blvd", "Lemon Cir", "Mango Pl", "Papaya Dr", "Coconut Way",
                "Pineapple St", "Cherrywood Ave", "Blossom Rd", "Rose Ln", "Daisy St",
                "Lilac Blvd", "Tulip Cir", "Iris Pl", "Violet Dr", "Lavender Way",
                "Jasmine St", "Marigold Ave", "Sunflower Rd", "Dahlia Ln", "Azalea St",
                "Orchid Blvd", "Lotus Cir", "Lily Pl", "Peony Dr", "Hyacinth Way",
                "Camellia St", "Narcissus Ave", "Gardenia Rd", "Begonia Ln", "Zinnia St",
                "Amaryllis Blvd", "Buttercup Cir", "Bluebell Pl", "Ivy Dr", "Fern Way",
                "Clover St", "Mint Ave", "Sage Rd", "Basil Ln", "Thyme St"
            ]        
            return random.choice(streets)
    
    def _generate_random_city(self):
            cities = [
        "Springfield", "Rivertown", "Lakeview", "Hillside", "Sunnydale",
        "Mapleton", "Brookfield", "Meadowvale", "Oakwood", "Crestview",
        "Ashton", "Fairview", "Lakeside", "Greenwood", "Westbrook",
        "Eastwood", "Northville", "Southport", "Riverdale", "Bayside",
        "Harbortown", "Pinecrest", "Cedarville", "Hightower", "Clearwater",
        "Silverton", "Elmwood", "Brighton", "Kingsley", "Queensborough",
        "Avalon", "Rockport", "Seaview", "Newhaven", "Edgewater",
        "Sunsetville", "Willowbrook", "Maplewood", "Ivybridge", "Birchwood",
        "Stonebridge", "Waterfall", "Eaglewood", "Hawthorne", "Bridgewater",
        "Rosewood", "Foxglove", "Magnolia", "Fernwood", "Sycamore",
        "Timberline", "Mistwood", "Wolf Creek", "Riverside", "Lakewood",
        "Starfield", "Winterhaven", "Summerdale", "Autumnwood", "Springwood",
        "Stormville", "Cloudview", "Sundale", "Moonville", "Starville",
        "Frostburg", "Rainwood", "Windyridge", "Snowfall", "Sunshine City",
        "Thunderbay", "Dewpoint", "Breezewood", "Drysdale", "Heatwave",
        "Coldwater", "Mildtown", "Humid Heights", "Warm Springs", "Chilltown",
        "Tornado Alley", "Cyclone City", "Blizzard Bluff", "Lightning Ridge", "Hurricane Harbor",
        "Tidepool", "Oceanview", "Riverbend", "Lakefront", "Bayshore",
        "Cliffside", "Beachwood", "Sandstone", "Duneville", "Coral Cove",
        "Marinaview", "Shoalwater", "Islandton", "Seabreeze", "Wavecrest",
        "Pearlport", "Anchor Bay", "Mariner's Cove", "Sailor's Retreat", "Fisherman's Wharf",
        "Mermaid's Lagoon", "Pirate's Cove", "Treasure Island", "Shipwreck Shore", "Nautical Bay",
        "Lighthouse Point", "Harborview", "Seashell Beach", "Starfish City", "Coral Reef",
        "Dolphin Bay", "Whale Watch", "Seal Shore", "Pelican Point", "Albatross Island",
        "Flamingo Flats", "Heron Haven", "Parrot's Perch", "Toucan Town", "Peacock Plains",
        "Gull's Nest", "Penguin Park", "Falcon Heights", "Eagle's Eyrie", "Hawk Hill",
        "Vulture Valley", "Owl's Nest", "Raven Ridge", "Swan Lake", "Duck Pond",
        "Goose Green", "Pheasant Run", "Quail Hollow", "Turkey Trot", "Stork Station",
        "Sparrow Springs", "Cardinal Corner", "Robin's Roost", "Wren's Rest", "Jay Junction",
        "Magpie Meadow", "Finch Field", "Lark Landing", "Hummingbird Haven", "Kingfisher Keep"
    ]
            return random.choice(cities)

    def _generate_random_country(self):
        countries = [
    "USA", "Canada", "Mexico", "Guatemala", "Belize", "El Salvador", 
    "Honduras", "Nicaragua", "Costa Rica", "Panama", "UK", "Ireland", 
    "France", "Germany", "Netherlands", "Belgium", "Luxembourg", 
    "Switzerland", "Austria", "Liechtenstein", "Monaco", "Spain", 
    "Portugal", "Italy", "Malta", "Denmark", "Norway", "Sweden", 
    "Finland", "Iceland", "Australia", "China", "Japan", "South Korea", 
    "India", "Singapore", "Malaysia", "Thailand", "Vietnam", "Indonesia",
    "Philippines"
    ]
        return random.choice(countries)

class Products:
    def __init__(self, db):
        self.db = db
        self.cnxpool = db.cnxpool

    def get_categories(self):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT * FROM Categories")
            return _convert_to_json(cursor)
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            connection.close()
            cursor.close()

    def get_product(self,limit, product_identifier=None):

        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
    
        try:
            if product_identifier is None:
                query = f"SELECT * FROM Products ORDER BY product_id DESC {_get_limit(limit)}"
            elif isinstance(product_identifier, int):
                query = "SELECT * FROM Products WHERE product_id = %s"
            elif product_identifier == 'random':
                query = "SELECT * FROM Products ORDER BY RAND() LIMIT 1"

            if product_identifier is None or product_identifier == 'random':
                cursor.execute(query)
                return _convert_to_json(cursor)
            else:
                cursor.execute(query, (product_identifier,))
                return _convert_to_json(cursor.fetchone())
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()

    def get_random_product(self, limit,user_identifier=None):

        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        dbname = 'ecomdb'
        products_ids =[]
        try:
            cursor.execute(f"SELECT min(product_id) AS first_product,max(product_id) AS last_product FROM {dbname}.Products")
            products = _convert_to_json(cursor)[0]
            for i in range(limit):
                products_ids.append(random.randint(products['first_product'], products['last_product']))
            return products_ids
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()           

class Orders:
    def __init__(self, db):
        self.db = db
        self.cnxpool = db.cnxpool

    def generate_order(self,order_start_date,order_end_date):
        # print("adding an order")
        dbname = 'ecomdb'
        cnxpool = self.cnxpool
        connection = _get_connection_with_retry(cnxpool)
        # connection = cnxpool.get_connection()
        cursor = connection.cursor()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
            #Get Products
            cursor.execute(f"SELECT product_id, price FROM {dbname}.Products;")
            products = cursor.fetchall()
            # print(products)
            #Select a random user
            cursor.execute(f"SELECT min(user_id) AS first_user,max(user_id) AS last_user FROM {dbname}.Users")
            users = _convert_to_json(cursor)[0]
            user_id = random.randint(users['first_user'], users['last_user']) 
            # print(user_id)
            # Create an order entry
            # end_date = datetime.now().date()
            # end_date =  datetime(end_date.year , 4, 28).date()
            start_date =  datetime.strptime(order_start_date, '%Y-%m-%d').date() if order_start_date else  datetime.now().date()
            print(start_date)
            end_date =  datetime.strptime(order_end_date, '%Y-%m-%d').date() if order_end_date else datetime.now().date()
            print(end_date)
            order_date = _random_date(start_date, end_date)            
            print(order_date)
            # order_date = datetime.now().date()
            status = random.choice(["Processing", "Shipped", "Delivered", "Cancelled"])
            cursor.execute(f"INSERT INTO {dbname}.Orders (user_id, order_date, total_price, status) VALUES ({user_id}, '{order_date}', 0, '{status}');")
            order_id = cursor.lastrowid

            # Randomly select products for this order
            order_products = random.sample(products, random.randint(1, 5))
            total_price = 0
            values_list = []
            for product_id, price in order_products:
                quantity = random.randint(1, 5)
                total_price += price * quantity

                # Insert into OrderItems
                # cursor.execute(f"INSERT INTO ecomdb.OrderItems (order_id, product_id, quantity, price_at_time_of_purchase) VALUES ({order_id}, {product_id}, {quantity}, {price});")
                values_list.append(f"({order_id}, {product_id}, {quantity}, {price})")

            # Update total_price in Orders
            insert_query = f"INSERT INTO {dbname}.OrderItems (order_id, product_id, quantity, price_at_time_of_purchase) VALUES {', '.join(values_list)};"
            cursor.execute(insert_query)
                            
            cursor.execute(f"UPDATE {dbname}.Orders SET total_price = {total_price} WHERE order_id = {order_id};")

            # Insert into Payments
            payment_date = order_date
            payment_type = random.choice(["Credit Card", "Debit Card", "PayPal", "Cash"])
            payment_status = random.choice(["Paid", "Pending", "Failed"])
            cursor.execute(f"INSERT INTO {dbname}.Payments (order_id, payment_date, payment_type, payment_status) VALUES ({order_id}, '{payment_date}', '{payment_type}', '{payment_status}');")            
            connection.commit()
            return int(cursor.lastrowid)
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()

    def delete_order(self, order_id):
        dbname = 'ecomdb'
        cnxpool = self.cnxpool
        connection = _get_connection_with_retry(cnxpool)
        cursor = connection.cursor()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
            # Start transaction
            connection.begin()

            # Delete from Payments table
            cursor.execute(f"DELETE FROM {dbname}.Payments WHERE order_id = %s;", (order_id,))

            # Delete from OrderItems table
            cursor.execute(f"DELETE FROM {dbname}.OrderItems WHERE order_id = %s;", (order_id,))

            # Delete from Orders table
            cursor.execute(f"DELETE FROM {dbname}.Orders WHERE order_id = %s;", (order_id,))

            # Commit the transaction
            connection.commit()
            print(f"Order {order_id} and its related records were successfully deleted.")
        except Exception as e:
            connection.rollback()
            print(f"An error occurred: {e}")
        finally:
            cursor.close()
            connection.close()

    # create a new function to update the status of an order
    def update_order_status(self, order_id, new_status):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
            query = f"UPDATE Orders SET status = %s WHERE order_id = %s"
            cursor.execute(query, (new_status, order_id))
            connection.commit()
            print(f"Order {order_id} status updated to {new_status}")
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()

    #define fonction to get all orders with an optinal parameter to get a single order
    def get_orders(self, limit,order_id=None,order_field='order_id'):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
            if order_id is None:
                query = f"SELECT * FROM Orders order by {order_field} DESC {_get_limit(limit)}"
                cursor.execute(query)
                return _convert_to_json(cursor)
            elif isinstance(order_id, int):
                query = f"SELECT * FROM Orders WHERE order_id = %s"
                cursor.execute(query, (order_id, ))
                return _convert_to_json(cursor)
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()
            # return _convert_to_json(cursor.fetchone())
            # return _convert_to_json(cursor)
            # return cursor.fetchall()

class Reviews:
    def __init__(self, db):
        self.db = db
    def generate_reviews_for_random_orders(self, num_reviews):

        connection, cursor = self.db.connect()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
        # Fetch all orders and associated products
            cursor.execute("SELECT o.order_id, o.user_id, oi.product_id FROM ecomdb.Orders o JOIN ecomdb.OrderItems oi ON o.order_id = oi.order_id;")
            all_orders = cursor.fetchall()
            selected_orders = random.sample(all_orders, min(num_reviews, len(all_orders)))
            values_list = []
            for order_id, user_id, product_id in selected_orders:
                rating = random.randint(1, 5)
                comments = ["Great product!", "satisfied.", "Could be better.", "Not as expected.", "I love it!", "Would not recommend."]
                comment = random.choice(comments)
                end_date = datetime.now().date()
                start_date =  datetime(end_date.year , 1, 1).date()
                review_date = _random_date(start_date, end_date)
                print(review_date)
                values_list.append(f"({product_id}, {user_id}, {rating}, '{comment}', '{review_date}')")
                # Insert the review
                
                # self.db.cursor.execute(f"INSERT INTO ecomdb.Reviews (product_id, user_id, rating, comment, review_date) VALUES ({product_id}, {user_id}, {rating}, '{comment}', '{review_date}');")

            # Commit the transaction
            insert_query = f"INSERT INTO ecomdb.Reviews (product_id, user_id, rating, comment, review_date) VALUES {', '.join(values_list)};"
            cursor.execute(insert_query)                
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()

    # define get_reviews function with limit
    def get_review(self, limit, product_id=None):
        cnxpool = self.db.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
            if product_id is None:
                query = f"SELECT * FROM Reviews order by review_id DESC {_get_limit(limit)}"
                cursor.execute(query)
                return _convert_to_json(cursor)
            elif isinstance(product_id,int):
                query = f"SELECT * FROM Reviews WHERE product_id = %s"
                cursor.execute(query, (product_id,))
                return _convert_to_json(cursor.fetchone())
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            # print(connection)
            connection.close()
            cursor.close()
            # return _convert_to_json(cursor.fetchone())

class CustomerSupport:
    def __init__(self, db):
        self.db = db
        self.cnxpool = db.cnxpool

    def insert_random_support_tickets(self, num_tickets,user_ids,product_ids):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()

        try:
            for i in range(num_tickets):
                user_id = user_ids[i]
                product_id = product_ids[i]
                issue_type = random.choice(["Delivery Issue", "Product Defect", "Warranty Claim", "Technical Support", "Other"])
                description = "Generated issue description."
                creation_date = datetime.now() - timedelta(days=random.randint(0, 60))
                
                status = random.choice(["Open", "In Progress", "Pending Customer", "Resolved"])
                if status == 'Resolved':
                    resolution_date = creation_date + timedelta(days=random.randint(1, 30))
                resolution_date = None
                query = "INSERT INTO SupportTickets (user_id, product_id, issue_type, description, creation_date, resolution_date, status) " \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (user_id, product_id, issue_type, description, creation_date, resolution_date, status))
            
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"An error occurred: {e}")
        finally:
            connection.close()
            cursor.close()

    def insert_random_customer_feedback(self, num_feedbacks,user_id,product_id):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()

        try:
            for _ in range(num_feedbacks):
                # user_id = self._get_random_user_id(cursor)
                # product_id = self._get_random_product_id(cursor)
                rating = random.randint(1, 5)
                comment = "Sample feedback comment."
                submission_date = datetime.now() - timedelta(days=random.randint(0, 60))

                query = "INSERT INTO CustomerFeedback (user_id, product_id, rating, comment, submission_date) " \
                        "VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(query, (user_id, product_id, rating, comment, submission_date))

            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"An error occurred: {e}")
        finally:
            connection.close()
            cursor.close()

    def insert_random_customer_queries(self, num_queries,user_id):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()

        try:
            for _ in range(num_queries):
                # user_id = self._get_random_user_id(cursor)
                query_type = random.choice(["General Inquiry", "Product Inquiry", "Order Status", "Complaint", "Other"])
                query_text = "Sample query text."
                submission_date = datetime.now() - timedelta(days=random.randint(0, 60))

                query = "INSERT INTO CustomerQueries (user_id, query_type, query_text, submission_date) " \
                        "VALUES (%s, %s, %s, %s)"
                cursor.execute(query, (user_id, query_type, query_text, submission_date))

            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"An error occurred: {e}")
        finally:
            connection.close()
            cursor.close()

    def get_tickets(self, limit, ticket_id=None):
        cnxpool = self.db.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
            if ticket_id is None:
                query = f"SELECT * FROM SupportTickets order by ticket_id DESC {_get_limit(limit)}"
                cursor.execute(query)
                return _convert_to_json(cursor)
            elif isinstance(ticket_id,int):
                query = f"SELECT * FROM SupportTickets WHERE ticket_id = %s"
                cursor.execute(query, (ticket_id,))
                return _convert_to_json(cursor.fetchone())
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            connection.close()
            cursor.close()

    def resolve_support_tickets(self, num_tickets):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()

        try:
                # query = "INSERT INTO SupportTickets (user_id, product_id, issue_type, description, creation_date, resolution_date, status) " \
                        # "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            query = f"UPDATE SupportTickets SET resolution_date = DATE_ADD(creation_date, INTERVAL 10 DAY), status = 'Resolved' WHERE ticket_id IN (SELECT ticket_id FROM (SELECT ticket_id FROM SupportTickets WHERE status <> 'Resolved' ORDER BY RAND() LIMIT {num_tickets}) AS randomRows);"
            print(query)
            cursor.execute(query)
            
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"An error occurred: {e}")
        finally:
            connection.close()
            cursor.close()

class SupplyChain:
    def __init__(self, db):
        self.db = db
        self.cnxpool = db.cnxpool

    def create_inventory(self, product_ids):
        cnxpool = self.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()

        try:
            for product_id in product_ids:
                #select random number between 1 and 10
                num_products = random.randint(0, 100)
                warehouse_id = random.randint(1, 10)
                reorder_level = random.randint(5, 50)

                insert_query = f"""
                INSERT INTO Inventory (product_id, warehouse_id, quantity_available,reorder_level)
                VALUES ({product_id}, {warehouse_id}, {num_products},{reorder_level})
                """
                cursor.execute(insert_query)

                connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"An error occurred: {e}")
        finally:
            connection.close()
            cursor.close()

    def get_inventory(self, limit, ticket_id=None):
        cnxpool = self.db.cnxpool
        connection = cnxpool.get_connection()
        cursor = connection.cursor()
        if not connection or not cursor:
            print("Failed to connect to the database")
            return
        try:
            if ticket_id is None:
                query = f"SELECT * FROM Inventory order by inventory_id DESC {_get_limit(limit)}"
                cursor.execute(query)
                return _convert_to_json(cursor)
            elif isinstance(ticket_id,int):
                query = f"SELECT * FROM Inventory WHERE inventory_id = %s"
                cursor.execute(query, (ticket_id,))
                return _convert_to_json(cursor.fetchone())
        except Exception as e:
            connection.rollback()
            print(f"An error occured : {e}")
        finally:
            connection.close()
            cursor.close()

@staticmethod
def _random_date(start, end):
    """
    Generate a random date between `start` and `end`
    """
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

@staticmethod
def _convert_to_json(cursor):
    columns = [column[0] for column in cursor.description]
    results = cursor.fetchall()
    json_results = [dict(zip(columns, row)) for row in results]
    return json_results
    # return json.dumps(json_results, default=str)

def _get_limit(limit):
    if isinstance(limit,int):
        return f" LIMIT {limit}"
    else:
        return ''
@staticmethod
def _get_connection_with_retry(pool, retries=5, delay=1):
    for attempt in range(retries):
        try:
            return pool.get_connection()
        except mysql.connector.errors.PoolError as err:
            if attempt < retries - 1:
                time.sleep(delay)  # Wait for a bit before retrying
                print(f"No more connection. Attempt to get a connection : {attempt}")
                continue
            else:
                raise  # Reraise the exception if all retries failed        