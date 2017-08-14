#!/usr/local/bin/python
import datetime
import sys
import os
import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials

tld_list=['.ca','.cl','.co.nz','.co.uk','.com.ar','.com.br','.com.mx','.es','.fr','.hk','.ie','.in','.it','.nl','.pt','.se','.sg', 'ALL']
presto_cmd = '/usr/local/bin/presto --server presto.prod.dataf.eb:8080 --file %s > %s --output-format CSV_HEADER'


def load_creds():
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
    gc = gspread.authorize(credentials)
    return gc.open('Spectrum Metrics')



def run(year, month, sheet):
	current_month = datetime.date(year, month, 1)
	next_month = current_month + datetime.timedelta(days=31)

	template = open('spectrum_metrics_tmpl.sql', 'r').read()

	for tld in tld_list:
		process_tld(tld, current_month, next_month, template, sheet)

def process_tld(tld, current_month, next_month, template, sheet):
	curr_month_str = current_month.strftime('%Y-%m')
	next_month_str = next_month.strftime('%Y-%m')

	worksheet_title = '%s %s data' % (curr_month_str, tld)
	try:
		worksheet = sheet.add_worksheet(title=worksheet_title, rows="200", cols="10")
	except gspread.exceptions.RequestError as err:
		print 'Sheet %s already exists' % worksheet_title
		return

	sql = template.replace('[curr_month]', curr_month_str)
	sql = sql.replace('[next_month]', next_month_str)
	if tld == 'ALL':
		sql = sql.replace('[tld]', '1=1')
	else:	
		sql = sql.replace('[tld]', "tld='%s'" % tld)

	sqlfile = 'spectrum_metrics_%s%s.sql' % (curr_month_str, tld)
	csvfile = 'spectrum_metrics_%s%s.csv' % (curr_month_str, tld)

	open(sqlfile, 'w').write(sql)

	cmd = presto_cmd % (sqlfile, csvfile)
	print 'running....', cmd
	output = os.popen(cmd).read()
	print '%s %s: %s' % (curr_month_str, tld, output)
	os.remove(sqlfile)

	try:
		update_sheet(sheet, worksheet, curr_month_str, tld, csvfile)
	except gspread.exceptions.RequestError as err:
		sheet = load_creds()
		sheet.del_worksheet(worksheet)
		worksheet = sheet.add_worksheet(title=worksheet_title, rows="200", cols="10")
		process_tld(tld, current_month, next_month, template, sheet)

def update_sheet(sheet, worksheet, month, tld, csvfile):

	rows = []

	with open(csvfile, 'rb') as csvfile:
		reader = csv.reader(csvfile)
		for row_idx, row in enumerate(reader):
			rows.append(list(row))

	if len(rows) == 0:
		return

	cell_list = worksheet.range('A1:H%s' % len(rows))
	for cell in cell_list:
		cell.value = rows[cell._row - 1][cell._col - 1].decode('utf-8')
	worksheet.update_cells(cell_list)


if __name__ == '__main__':
	sheet = load_creds()
	if sys.argv[1] == 'custom':
		for month in (7,6,5,4,3,2,1):
			run(2017, month, sheet)
	elif len(sys.argv) > 2:
		run(int(sys.argv[1]), int(sys.argv[2]), sheet)
	else:
		today = datetime.datetime.today()
		yesterday = today - datetime.timedelta(days=1)
		run(yesterday.year, yesterday.month)

