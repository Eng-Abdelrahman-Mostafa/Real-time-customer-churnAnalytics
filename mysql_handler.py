import mysql.connector

class MySQLHandler:
    
    host = 'localhost'
    user='root'
    password=''
    database='churnAnalytics'

    def __init__(self, host=None, user=None, password=None, database=None):
        if host is None:
            host = MySQLHandler.host
        if user is None:
            user = MySQLHandler.user
        if password is None:
            password = MySQLHandler.password
        if database is None:
            database = MySQLHandler.database
        
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.connection.rollback()
            return False

    def fetch_data(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            result = self.cursor.fetchall()
            return result
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def close_connection(self):
        self.cursor.close()
        self.connection.close()