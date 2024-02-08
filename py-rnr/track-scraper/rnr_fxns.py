# load libraries
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import yaml
import psycopg2

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

def get_event_results(url):
    # Get HTML content from the URL
    response = requests.get(url)
    html = response.content

    # Parse HTML using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extract meet details
    meet_name = soup.select_one('h3 a').text.strip()

    # Get location info
    loc_info = soup.find_all(class_ = "panel-heading-normal-text inline-block")
    meet_date = loc_info[0].text
    meet_loc = loc_info[1].text
    meet_track_type = loc_info[2].text
    # Check if at altitude 
    if len(loc_info) > 3:
        meet_altitude = loc_info[3].text
    else: 
        meet_altitude = None 

    # Extract race names
    race_names = [h3.text.strip() for h3 in soup.find_all('h3')]
    race_names = race_names[1:]  # Drop the first race name (same as meet name)
    
    # Find all tables and convert them to DataFrames
    tables = pd.read_html(response.text)
    
    # Initialize an empty DataFrame for race results
    race_results = pd.DataFrame()

    for i, table in enumerate(tables):
        # Select specific columns (assuming PL, NAME, YEAR, TEAM, TIME exist in the table)
        temp_results = table[['PL', 'NAME', 'YEAR', 'TEAM', 'TIME']]
        
        # Add race name, check if race_names has enough elements to avoid index errors
        if i < len(race_names):
            temp_results['RACE_NAME'] = race_names[i]
        else:
            temp_results['RACE_NAME'] = 'Unknown Race'

        # Append to the main DataFrame
        race_results = pd.concat([race_results, temp_results], ignore_index=True)

    # Add meet details to the DataFrame
    race_results['MEET_DATE'] = meet_date
    race_results['MEET_LOCATION'] = meet_loc
    race_results['MEET_NAME'] = meet_name
    race_results['MEET_TRACK_TYPE'] = meet_track_type
    race_results['MEET_ALTITUDE'] = meet_altitude

    # Drop duplicates if necessary
    race_results = race_results.drop_duplicates()

    # Filter out any "UNKNOWN RACE" as these are duplicates of the prelim data
    race_results = race_results[race_results['RACE_NAME'] != 'Unknown Race']

    return race_results