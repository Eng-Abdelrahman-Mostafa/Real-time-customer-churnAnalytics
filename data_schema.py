import random

class DataSchema:
    # Function to generate random customer data
    @staticmethod
    def generate_random_data():
        Churned = random.choice([True, False])
        return {
            "CustomerID": str(random.randint(1000, 9999)),
            "Age": random.randint(18, 60),
            "Gender": random.choice(["Male", "Female"]),
            "Location": random.choice(["CityA", "CityB", "CityC"]),
            "ServiceUsage": {
                "Duration": random.randint(30, 120),
                "NumCalls": random.randint(50, 200),
                "NumMessages": random.randint(10, 50),
                "DataUsage": round(random.uniform(1.0, 5.0), 2)
            },
            "BillingInfo": {
                "MonthlyCharges": round(random.uniform(30.0, 100.0), 2),
                "PaymentMethod": random.choice(["CreditCard", "PayPal", "BankTransfer"])
            },
            "ChurnStatus": {
                "Churned": Churned,
                "ChurnDate": str(random.randint(1, 30)) + "-11-2023" if Churned else ""
            }
        }
    @staticmethod
    def schema():
        return {
            "CustomerID": "string",
            "Age": "integer",
            "Gender": "string",
            "Location": "string",
            "ServiceUsage": {
                "Duration": "integer",
                "NumCalls": "integer",
                "NumMessages": "integer",
                "DataUsage": "double"
            },
            "BillingInfo": {
                "MonthlyCharges": "double",
                "PaymentMethod": "string"
            },
            "ChurnStatus": {
                "Churned": "boolean",
                "ChurnDate": "string"
            }
        }

# customer_data = {
#     "CustomerID": "123",
#     "Age": 30,
#     "Gender": "Male",
#     "Location": "CityA",
#     "ServiceUsage": {
#         "Duration": 50,
#         "NumCalls": 100,
#         "NumMessages": 20,
#         "DataUsage": 2.5
#     },
#     "BillingInfo": {
#         "MonthlyCharges": 50.0,
#         "PaymentMethod": "CreditCard"
#     },
#     "ChurnStatus": {
#         "Churned": False,
#         "ChurnDate": ""
#     }
# }