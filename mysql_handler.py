import mysql.connector

class MySQLHandler:
    
    host = 'localhost'
    user='root'
    password=''
    database='churnAnalytics'
    # Function to save DataFrame to MySQL using MySQLHandler
    def save_to_mysql(dataframe, table_name):
        dataframe.persist()  # Optional: Persist the DataFrame to improve performance if necessary
        dataframe.write \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://localhost/churnAnalytics") \
            .option("dbtable", table_name) \
            .option("user", MySQLHandler.user) \
            .option("password", MySQLHandler.password) \
            .mode("overwrite") \
            .save()
        dataframe.unpersist()