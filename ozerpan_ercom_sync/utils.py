import frappe
import pymysql


def get_mysql_connection():
    config = frappe.conf

    try:
        # Create a connection to the MySQL database using pymysql
        connection = pymysql.connect(
            host=config["ercom_db_host"],
            database=config["ercom_db_name"],  # pymysql uses "db" instead of "database"
            user=config["ercom_db_user"],
            password=config["ercom_db_password"],
            charset="utf8mb4",  # Optional: Ensure proper encoding
            cursorclass=pymysql.cursors.DictCursor,  # Optional: Return rows as dictionaries
        )
        print("\n\n\nConnected to DB Successfully.\n\n\n")
    except pymysql.MySQLError as err:
        print(f"Error connecting to MySQL database: {err}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    return connection
