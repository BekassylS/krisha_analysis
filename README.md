# krisha_analysis
🏘️ Real Estate Data Analysis from Krisha.kz
This project is an exploratory data analysis (EDA) of real estate listings scraped from Krisha.kz — a popular platform for buying and renting property in Kazakhstan.

📦 Project Structure
raw_data_krisha.csv: The data that was scraped using the code in krisha_scraper.py and initially used.

krisha_data_after_DC.csv: The data after data cleaning process presented in Krisha_data_cleaning.sql. 

python_scraping_code.py: Python script that uses BeautifulSoup to scrape property listings data directly from Krisha.kz.

Krisha_data_cleaning.sql: SQL file containing the logic used to clean and prepare the raw dataset — including handling missing values and logically inferring incomplete entries.

krisha_eda.sql: (coming soon) SQL code used for the exploratory analysis itself.

krisha_eda_summary.md / eda_summary.txt: (coming soon) A short but insightful summary of the findings from the EDA stage.

🔧 What’s Done So Far
✅ Scraped raw property data using Python and BeautifulSoup

✅ Cleaned the data using SQL — handled missing values by applying logical rules

⏳ Currently working on EDA (will be shared soon)

📌 Notes
This is a work-in-progress project aimed at better understanding housing market patterns using real-world data. Final insights and visualizations will be added after the EDA is complete.
