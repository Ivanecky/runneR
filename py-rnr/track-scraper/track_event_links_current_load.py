# load libraries
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import yaml
import psycopg2

# connect to main tfrrs page
base_url = "https://tfrrs.org/results_search.html"

# placeholder for connecting to postgres
# placeholder for reading yaml file

def get_page_lnks(base_url):
    # extract current links from the page
    try:
        # get html
        html = requests.get(base_url)
    except:
        print("Issue making request")

    # get all links
    # convert to soup
    pg_soup = BeautifulSoup(html.content, 'html.parser')

    # get links
    lnks = pg_soup.find_all('a', href=True)

    # filter to only meet links
    lnks = [l for l in lnks if 'results' in l['href']]
    lnks = [l for l in lnks if 'results_search' not in l['href']]

    # fix link formatting
    lnks = ['https://tfrrs.org' + str(l['href']) for l in lnks]

    # return links
    return(lnks)

# iterate over pages in tfrrs results page
def get_all_lnks():

    base_url = "https://tfrrs.org/results_search.html?page="

    # list to hold links
    all_lnks = []

    # iterate over a range
    for i in range(1, 2000):
        # set url
        url = base_url + str(i)

        # print status
        print(f"Getting data for: {url}")

        # try and get data
        try:
            tmp_lnks = get_page_lnks(url)
        except:
            print(f"Issue getting data for {url}")
        
        # check to see if valid list of links
        if len(tmp_lnks) > 0:
            all_lnks.append(tmp_lnks)
        else:
            print(f"Page {base_url} did not have a valide number of links. Returning data now...")
            all_lnks = [s for sublist in all_lnks for s in sublist]
            return(all_lnks)
        
    # flatten out list of lists
    all_lnks = [s for sublist in all_lnks for s in sublist]
    
    # return final list
    print(f"All data gathered. Returning links...")
    return(all_lnks)

# function to get data from links
def get_evnt_lnks(url):
    # get html for base url
    try:
        # get html
        html = requests.get(url)
    except:
        print(f"Issue making request for {url}")

    # get all links
    # convert to soup
    pg_soup = BeautifulSoup(html.content, 'html.parser')

    # get links
    lnks = pg_soup.find_all('a', href=True)

    # filter to only meet links
    lnks = [l for l in lnks if 'results' in l['href']]
    lnks = [l for l in lnks if 'results_search' not in l['href']]

    # keep only href elements
    lnks = [l['href'] for l in lnks]

    # return links
    return(lnks)

def get_tf_evnt_lnks(lnks):
    # test for getting track event links
    # subset to track only
    tf_lnks = [l for l in all_lnks if 'results/xc' not in l]

    # vector to hold track links
    tf_evnt_lnks = []

    # iterate over subset to get the event links
    for i in tf_lnks:
        # print status
        print(f"Getting data for {i}")

        # get the event links
        try:
            tmp_lnks = get_evnt_lnks(i)
        except:
            print(f"Error getting data for {i}")
        
        # append the event links to the overall list
        tf_evnt_lnks.append(tmp_lnks)

    # unnest list of lists
    tf_evnt_lnks = [s for sublist in tf_evnt_lnks for s in sublist]

    # return the data
    return(tf_evnt_lnks)

def main():
    # get all links on tfrrs
    all_lnks = get_all_lnks()

    # get tf events
    tf_evnts = get_tf_evnt_lnks(all_lnks)

    # Read connection data from YAML
    with open("/Users/samivanecky/git/runneR/secrets/aws_rds.yaml", "r") as yaml_file:
        pg_config = yaml.safe_load(yaml_file)

    # Connect to the database
    conn = psycopg2.connect(
        host=pg_config['host'],
        user=pg_config['user'],
        password=pg_config['pwd'],
        dbname=pg_config['dbname'],
        port=pg_config['port']
    )