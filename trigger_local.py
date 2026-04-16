import sys
sys.path.append('functions/ingest')
from main import process_file

class MockEvent:
    def __init__(self, data):
        self.data = data

files = [
  'portco-alpha/Portco_Alpha_KPI_Monthly_Jan2023_Jun2025.xlsx',
  'portco-beta/Portco_Beta_KPI_Monthly_Jul2023_Jun2025.xlsx',
  'portco-beta/Beta_Monthly_Report_Messy_Format.csv',
]
for f in files:
    try:
        process_file(MockEvent({'bucket': 'averroes-portfolio-intel-portfolio-data', 'name': f}))
    except Exception as e:
        print(f"Failed {f}: {e}")
