# Danford Compton
# CS410 Data Engineering
# Data Gathering program for Breadcrumbs project

# need these
import requests
import time

# this is the url to get the huge JSON file
url = 'http://rbi.ddns.net/getBreadCrumbData'
# this is to name the file
today = time.strftime("%Y%m%d")
# this gets the file from the URL
# This operation takes a while to download, so be patient if it seems to be hung up
response = requests.get(url)
# Creates new file with today's date as the name, writes JSON to the file
with open(today, 'w') as fw:
    fw.write(response.text)
