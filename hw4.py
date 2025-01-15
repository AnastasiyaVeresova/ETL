import pandas as pd
import matplotlib.pyplot as plt

# Load the Excel file
excel_file = "F:/x/ETL/info/s4_2.xlsx"

# Read the sheets into DataFrames
df_120k = pd.read_excel(excel_file, sheet_name="120K")
df_150k = pd.read_excel(excel_file, sheet_name="150K")
df_250k = pd.read_excel(excel_file, sheet_name="250K")
df_300k = pd.read_excel(excel_file, sheet_name="300K")

# Plot the graphs
plt.figure(figsize=(12, 8))

# Plot for 120K payment
plt.plot(df_120k["Месяц"], df_120k["Платеж по основному долгу"], label="Principal 120K", color="blue")
plt.plot(df_120k["Месяц"], df_120k["Платеж по процентам"], label="Interest 120K", color="lightblue")

# Plot for 150K payment
plt.plot(df_150k["Месяц"], df_150k["Платеж по основному долгу"], label="Principal 150K", color="green")
plt.plot(df_150k["Месяц"], df_150k["Платеж по процентам"], label="Interest 150K", color="lightgreen")

# Plot for 250K payment
plt.plot(df_250k["Месяц"], df_250k["Платеж по основному долгу"], label="Principal 250K", color="red")
plt.plot(df_250k["Месяц"], df_250k["Платеж по процентам"], label="Interest 250K", color="lightcoral")

# Plot for 300K payment
plt.plot(df_300k["Месяц"], df_300k["Платеж по основному долгу"], label="Principal 300K", color="purple")
plt.plot(df_300k["Месяц"], df_300k["Платеж по процентам"], label="Interest 300K", color="lavender")

# Set the title and labels
plt.title("Loan Repayment Schedule")
plt.xlabel("Month")
plt.ylabel("Amount")
plt.legend()
plt.grid(True)

# Show the plot
plt.show()
