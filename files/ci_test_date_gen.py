import csv
from faker import Faker
import random
from datetime import datetime
import os

# Initialize Faker
fake = Faker()


# Function to generate positive test data
def generate_positive_data():
    return {
        "Identifier": fake.random_number(digits=10, fix_len=True),
        "Surname": fake.last_name()[:50],
        "Given_name": fake.first_name()[:50],
        "Middle_initial": fake.first_name()[0] if random.choice([True, False]) else "",  # Optional
        "Suffix": fake.suffix() if random.choice([True, False]) else "",
        "Primary_street_number": fake.building_number(),
        "Primary_street_name": fake.street_name(),
        "City": fake.city(),
        "State": fake.state_abbr(),
        "Zipcode": fake.zipcode()[:5],
        "Primary_street_number_prev": fake.building_number(),
        "Primary_street_name_prev": fake.street_name(),
        "City_prev": fake.city(),
        "State_prev": fake.state_abbr(),
        "Zipcode_prev": fake.zipcode()[:5],
        "Email": fake.email(),
        "Phone": fake.phone_number(),
        "Birthmonth": random.choice(range(1, 13))  # Random month between 1-12
    }


# Function to generate negative test data
def generate_negative_data():
    return {
        "Identifier": fake.random_number(digits=random.choice([8, 11]), fix_len=True),  # Not 10 digits
        "Surname": "" if random.choice([True, False]) else fake.lexify('?' * 60),  # Null or more than 50 characters
        "Given_name": "" if random.choice([True, False]) else fake.lexify('?' * 60),  # Null or more than 50 characters
        "Middle_initial": fake.lexify('?' * random.choice([2, 5])),  # More than 1 character
        "Suffix": fake.lexify('?' * 10),  # Invalid suffix
        "Primary_street_number": fake.lexify('?' * random.choice([2, 20])),  # Invalid street number
        "Primary_street_name": "",
        "City": fake.lexify('?' * 60),  # Invalid city name
        "State": fake.lexify('?' * random.choice([3, 4])),  # More than 2 characters
        "Zipcode": fake.lexify('?' * random.choice([3, 4])),  # Less than 5 digits
        "Primary_street_number_prev": fake.lexify('?' * random.choice([2, 20])),
        "Primary_street_name_prev": "",
        "City_prev": fake.lexify('?' * 60),
        "State_prev": fake.lexify('?' * random.choice([3, 4])),
        "Zipcode_prev": fake.lexify('?' * random.choice([3, 4])),
        "Email": "invalid_email",
        "Phone": "invalid_phone_number",
        "Birthmonth": random.choice(range(13, 100))  # Invalid month
    }


# Function to write the data to a CSV file
def create_csv_file(positive_data_list, negative_data_list):
    # Combine positive and negative data
    combined_data = positive_data_list + negative_data_list

    # Define CSV headers
    headers = [
        "Identifier", "Surname", "Given_name", "Middle_initial", "Suffix",
        "Primary_street_number", "Primary_street_name", "City", "State", "Zipcode",
        "Primary_street_number_prev", "Primary_street_name_prev", "City_prev",
        "State_prev", "Zipcode_prev", "Email", "Phone", "Birthmonth"
    ]

    # Generate file name in ddmmyyyy format
    file_name = f"Contact_info_{datetime.now().strftime('%d%m%Y')}.csv"

    # # Define output path
    output_dir = os.getcwd()
    output_path = os.path.join(output_dir, file_name)

    # Write to CSV
    with open(file_name, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for row in combined_data:
            writer.writerow(row)

    print(f"CSV file '{file_name}' created successfully at '{output_path}'!")


# Function to generate both positive and negative data based on input number of records
def generate_data(num_records):
    positive_data_list = [generate_positive_data() for _ in range(num_records // 2)]  # Generate half positive records
    negative_data_list = [generate_negative_data() for _ in range(num_records // 2)]  # Generate half negative records
    return positive_data_list, negative_data_list


# Input: Number of records to generate
num_records = 100#int(input("Enter the number of records to generate: "))

# Ensure num_records is even (for equal split of positive/negative data)
if num_records % 2 != 0:
    num_records += 1
    print(f"Number of records adjusted to {num_records} to ensure even split of positive and negative data.")

# Generate data and create CSV file
positive_data, negative_data = generate_data(num_records)
create_csv_file(positive_data, negative_data)
