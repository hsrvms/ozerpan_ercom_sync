import mysql.connector

import frappe


def get_mysql_connection():
    config = frappe.conf

    try:
        connection = mysql.connector.connect(
            host=config["ercom_db_host"],
            database=config["ercom_db_name"],
            user=config["ercom_db_user"],
            password=config["ercom_db_password"],
        )
        print("\n\n\nConnected to DB Successfully.\n\n\n")
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL database: {err}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")

    return connection
