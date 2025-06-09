import csv

with open('CleanETHUSDT23-25.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    for i, row in enumerate(reader):
        if i < 20:  # Print first 20 rows
            print(f"Row {i}: {row}")
        else:
            break