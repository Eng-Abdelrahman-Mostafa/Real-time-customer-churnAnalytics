import random

class DataFaker:
    # Function to generate random customer data
    @staticmethod
    def generate_random_data():
        Churned = random.choice([True, False])
        return {
            "CustomerID": str(random.randint(1000, 1200)),
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
   
