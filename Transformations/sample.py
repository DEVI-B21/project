# sql_in_class_methods.py
# Static SQL queries inside class methods with print statements
# No dynamic SQL, no parameters, no string formatting

class OrderProcessor:
    def create_orders_table(self):
        print("Creating orders table")

        sql = """
        CREATE TABLE analytics.orders (
            order_id INT,
            customer_id INT,
            order_date DATE,
            total_amount DECIMAL(10,2)
        );
        """
        print("Orders table SQL prepared")
        print(sql)

    def insert_orders(self):
        print("Inserting data into orders table")

        sql = """
        INSERT INTO analytics.orders
        SELECT
            order_id,
            customer_id,
            order_date,
            total_amount
        FROM staging.orders;
        """
        print("Insert SQL prepared")
        print(sql)

    def select_orders(self):
        print("Selecting recent orders")

        sql = """
        SELECT
            order_id,
            customer_id,
            order_date,
            total_amount
        FROM analytics.orders
        WHERE order_date >= '2024-01-01';
        """
        print("Select SQL prepared")
        print(sql)


class SalesAggregator:
    def create_daily_summary(self):
        print("Creating daily sales summary table")

        sql = """
        CREATE TABLE analytics.daily_sales_summary AS
        SELECT
            order_date,
            COUNT(order_id) AS total_orders,
            SUM(total_amount) AS total_revenue
        FROM analytics.orders
        GROUP BY order_date;
        """
        print("Daily summary SQL prepared")
        print(sql)

    def find_high_value_customers(self):
        print("Finding high value customers")

        sql = """
        CREATE TABLE analytics.high_value_customers AS
        SELECT
            customer_id,
            SUM(total_amount) AS lifetime_value
        FROM analytics.orders
        GROUP BY customer_id
        HAVING SUM(total_amount) > 100000;
        """
        print("High value customers SQL prepared")
        print(sql)


class CustomerMaintenance:
    def update_customer_status(self):
        print("Updating customer status to VIP")

        sql = """
        UPDATE analytics.customer_status
        SET status = 'VIP'
        WHERE customer_id IN (
            SELECT customer_id
            FROM analytics.high_value_customers
        );
        """
        print("Update SQL prepared")
        print(sql)

    def delete_old_orders(self):
        print("Deleting old orders")

        sql = """
        DELETE FROM staging.old_orders
        WHERE order_date < '2020-01-01';
        """
        print("Delete SQL prepared")
        print(sql)
