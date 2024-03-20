import csv
import random

# List of student names
names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Harry", "Ivy", "Jack"]

# List of subjects
subjects = ["Math", "Science", "English", "History", "Geography"]

# Generate random scores for each student and subject
data = []
for name in names:
    for subject in subjects:
        score = random.randint(50, 100)  # Random score between 50 and 100
        data.append([name, subject, score])

# Write data to a CSV file
with open("student_scores.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Name", "Subject", "Score"])
    writer.writerows(data)

print("CSV file generated successfully!")

