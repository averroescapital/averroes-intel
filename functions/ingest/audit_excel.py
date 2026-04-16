import openpyxl
import os

file_path = 'local_MAfileFeb26.xlsx'
wb = openpyxl.load_workbook(file_path, data_only=True)
ws = wb['Financial KPIs']
print(f"Audit Results:")
for r in range(1, 15):
    row_data = [str(ws.cell(row=r, column=c).value)[:15].center(15) for c in range(1, 15)]
    print(f"R{r:02d}: {' | '.join(row_data)}")
