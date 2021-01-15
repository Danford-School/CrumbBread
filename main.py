# Danford Compton
# CS410 Data Engineering
# Data Gathering program for Breadcrumbs project

import requests
import time

url = 'http://rbi.ddns.net/getBreadCrumbData'

today = time.strftime("%Y%m%d")

response = requests.get(url)

with open(today, 'w') as fw:
    fw.write(response.text)
