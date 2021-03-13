# Butter knife because it's a scraper.

import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import json
import datetime


#################################################
# Danford Note:
# This isn't working because it is taking every character because it doesn't know that words are words, it just sees
# characters. Beautiful Soup should really have a solution for this.
# if not, we can pull out whitespace-divided characters and turn them into strings and either use them on the fly,
# or put them into an array or dict.
#################################################


url = "http://rbi.ddns.net/getStopEvents"
html = urlopen(url)

soup = BeautifulSoup(html, 'lxml')
type(soup)

all_trips = soup.find_all('h3')
clean_trips = BeautifulSoup(str(all_trips), 'lxml').get_text()
cleaner_trips = clean_trips.split()
trip_num_list = []
for trippy in cleaner_trips:
    if trippy.isnumeric():
        trip_num_list.append(trippy)

rows = soup.find_all('tr')
list_rows = []
for row in rows[1:]:
    cells = row.find_all('td')
    str_cells = str(cells)
    clean = re.compile('<.*?>')
    clean2 = (re.sub(clean, '',str_cells))
    list_rows.append(clean2)

col_labels = soup.find_all('th')

col_str = str(col_labels)
text = BeautifulSoup(col_str, "lxml").get_text()
# get all of our individual columns
x = text.split(',')
# strip the first and last [ ] from our list
x[0] = x[0].strip('[')
x[len(x)-1] = x[len(x)-1].strip(']')
x = list(map(str.strip, x))

# get rid of all duplicates (definitely overkill but works)
header_set = set()
headers = []
temp = ''
for item in x:
    item = item.strip(' ')
    if item not in header_set:
        header_set.add(item)
        headers.append(item)

###############
headers.append('trip_id')
headers.append('date')

data_list = []
for row in list_rows:
    row = row + ', , '
    data_list.append(row.split(', '))


df = pd.DataFrame(columns=headers, data=data_list)
#df = df[0].str.split(',', expand=True)

almost_the_date = soup.find("h1")
close_to_the_date = BeautifulSoup(str(almost_the_date), "lxml").get_text()
closer_to_the_date = close_to_the_date.split()
date_ish = str(closer_to_the_date[4])

# remove the [ character from all items in vehicle_number aka column 0
df['vehicle_number'] = df['vehicle_number'].str.strip('[')
# remove the ] character from all items in schedule_status aka column 22
df['schedule_status'] = df['schedule_status'].str.strip(']')

trip_list_index = 0
for index, row in df.iterrows():
    if row['vehicle_number'] == ']':
        #print("ahhhhh")
        trip_list_index += 1
        #df.drop(row)
    else:
        row['trip_id'] = trip_num_list[trip_list_index]
        row['date'] = date_ish

df = df[df['vehicle_number'] != ']']

json_data = df.to_json(orient="records")
parsed = json.loads(json_data)
today = datetime.date.today().strftime("%Y%m%d")
with open('stop' + today + '.json', 'w') as outfile:
    json.dump(parsed, outfile)
    outfile.close()

