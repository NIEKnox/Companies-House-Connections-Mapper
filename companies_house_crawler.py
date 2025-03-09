################
## Code written by NIEKnox and posted on GitHub on 9th March 2025; you can use this code for whatever non-commercial
## purposes you want as long as proper credit is given
################


# imports
from neo4j import GraphDatabase as gd
import requests
import json
import time
from datetime import datetime
from credentials import neo4j_uri, neo4j_username, neo4j_password, neo4j_database, companies_house_auth_header

# testing flags
testing_nodewrite_enabled = False
testing_relationshipwrite_enabled = False
testing_purgedb_enabled = False
purge_at_end = False

# creds
URI_neo4j = neo4j_uri
AUTH_neo4j = (neo4j_username, neo4j_password)

# verify connection
driver = gd.driver(URI_neo4j, auth=AUTH_neo4j)
driver.verify_connectivity()
print("Connection established.")

session = driver.session(database=neo4j_database)

### GENERAL FUNCTIONS
verbose = False
def verbose_logger(print_string, logging = verbose):
    if logging:
        print(print_string)


### NEO4J FUNCTIONS
# used to format properties properly for neo4j relationships
def properties_wrapper(properties):
    properties_query = "{"
    for key in properties:
        keystring = str(key)
        properties_query += keystring.strip("'") + ": \""
        properties_query += str(properties[key]) + "\", "
    properties_query = properties_query.rstrip(', ') + "}"

    return properties_query


# write node if does not exist. properties is a dict of property names and property info
def write_node(node_id, labels, properties, session=session):
    query = "MERGE ({}:".format(str(node_id))
    # only one label? then append immediately
    if type(labels) != list:
        query += str(labels) + " "
    else:
        query += "&".join(labels) + " "

    # add on properties
    properties_query = properties_wrapper(properties)
    query += properties_query + ")"

    try:
        session.run(query)
        verbose_logger("Node write query run!")
        return "Success"
    except:
        verbose_logger("Node write query failed!")
        return "Fail"

# works; can't have any names starting with numbers though
if testing_nodewrite_enabled:
    print(write_node("P001", ["person"], properties={"ID": "P001", "DOB": "1931", "Name": "Barbara KAHAN"}))
    print(write_node("P002", ["person"], properties={"ID": "P002", "DOB": "1967", "Name": "Avtar SINGH"}))
    print(write_node("P003", ["person"], properties={"ID": "P003","DOB": "1968", "Name": "Avtar SINGH2"}))
    print(write_node("C001", ["company"], properties={"ID": "C001","Name": "TSV PROJECTS LTD", "Status": "Active"}))
    print(write_node("C002", ["company"], properties={"ID": "C002","Name": "TSV PROJECTS LTD2", "Status": "Active"}))


# define relationship between nodes
def write_relationship(from_node_type, from_node_id, to_node_type, to_node_id, relationship_type, relationship_properties, session=session):
    # match nodes to their ids
    query = "MATCH "
    # from node
    query += "({}:{}".format(from_node_id, from_node_type) + "{ID: \"" + from_node_id + "\"}), "
    # to node
    query += "({}:{}".format(to_node_id, to_node_type) + "{ID: \"" + to_node_id + "\"})"
    # query
    query += "\nMERGE ({})-[r:{} {}]->({})".format(from_node_id, relationship_type,
                                              properties_wrapper(relationship_properties), to_node_id)

    # execute
    try:
        session.run(query)
        verbose_logger("Relationship write query run!")
        return "Success"
    except Exception as error:
        verbose_logger(f"Relationship write query failed!: {error}")
        return "Fail"


if testing_relationshipwrite_enabled:
    write_relationship("person", "P001", "company", "C001", "OFFICER",
                       {"role": "director", "status":"resigned"})


def purge_db(session=session):
    query = "MATCH (n) DETACH DELETE n"
    try:
        session.run(query)
        print("Purge run!")
        return "Success"
    except:
        print("Purge failed!")
        return "Fail"

if testing_purgedb_enabled:
    purge_db()


### COMPANIES HOUSE API FUNCTIONS
headers = {
    'Content-type': 'application/json',
    'Authorization': companies_house_auth_header,
}

base_url = 'https://api.company-information.service.gov.uk'

# improvements:
# 1. handle rate limiting
# 2. add in generic characteristic handler that can cope with missing data


# queries companies house using creds and returns the data
def query_handler(appended_url, headers = headers, base_url = 'https://api.company-information.service.gov.uk'):
    search_url = base_url + appended_url
    # TODO: add error handling? response 429
    data = requests.get(search_url, auth=None, headers=headers)
    while data.status_code == 429:
        print('zzz...')
        time.sleep(300)
        data = requests.get(search_url, auth=None, headers=headers)

    data_text_dict = json.loads(data.text)

    return data_text_dict

def write_node_officer(data):
    name = data['name']
    try:
        dob = str(data['date_of_birth']['year']) + '-' + str(data['date_of_birth']['month'])
    except:
        dob = 'Unknown'
    id = 'ID_' + data['etag']
    inactive_count = str(data['inactive_count'])
    resigned_count = str(data['resigned_count'])

    officer_properties = {'ID': id, 'name': name, 'dob': dob, 'inactive_count': inactive_count,
                  'resigned_count': resigned_count}

    # make node if not exists
    write_node(node_id=id, labels=['person'], properties=officer_properties, session=session)

    return officer_properties

# TODO: unify sic_codes handling across both, or allow overwrites?
def write_node_company(data, source):
    # get info on this company
    company_properties = {}
    if source == 'officer':
        # company_properties['address'] = company['address']
        company_properties['name'] = data['appointed_to']['company_name']
        company_properties['company_number'] = data['appointed_to']['company_number']
        company_id = 'ID_' + str(company_properties['company_number'])
        company_properties['ID'] = company_id
        company_properties['company_status'] = data['appointed_to']['company_status']
    elif source == 'company':
        company_properties['name'] = data['company_name']
        company_properties['company_number'] = data['company_number']
        company_id = 'ID_' + str(company_properties['company_number'])
        company_properties['ID'] = company_id
        company_properties['company_status'] = data['company_status']
    else:
        print('Company source not defined!')

    # make company node if not exists
    write_node(node_id=company_id, labels=['company'], properties=company_properties)

    return company_properties


# takes in the data for an officer, adds the node to the db, adds relationships to node, returns all connected companies
# TODO: handle officers with >35 appointments
def officer_handler(data=None):
    # handle missing data
    if data is None:
        print("No data has been passed to officer_handler!")
        return
    # get relevant info
    officer_properties = write_node_officer(data)

    officer_id = officer_properties['ID']
    companies_list = []

    # collect companies
    for company in data['items']:
        # add company to list to review
        companies_list.append(company['links']['company'] + "/officers")

        # make company node if not exists
        company_properties = write_node_company(company, source='officer')
        company_id = company_properties['ID']

        # define company relationships
        relationship_properties = {}
        # apparently appointed_on isn't always implicit lmao?
        try:
            relationship_properties['appointed_on'] = company['appointed_on']
        except:
            relationship_properties['appointed_on'] = 'Unknown'
        # not all have occupations
        try:
            relationship_properties['occupation'] = company['occupation']
        except:
            relationship_properties['occupation'] = "None"
        relationship_properties['officer_role'] = company['officer_role']

        write_relationship("person", officer_id, "company", company_id, "Officer",
                           relationship_properties, session=session)

    return companies_list

# test3 = query_handler('/officers/auRgqZX1stWO-EoEyget_Mle45c/appointments')
# print(test3)
# newlist = officer_handler(test3)
# print(newlist)

# takes in the data for a company, returns all connected officers. officer_handler builds all nodes
def company_handler(data=None):
    # handle missing data
    if data is None:
        print("No data has been passed to company_handler!")
        return

    # go to officers
    officer_append = data['links']['self']
    officer_data = query_handler(officer_append)

    officers_list = []

    for officer in officer_data['items']:
        # add officer to list to review
        officers_list.append(officer['links']['officer']['appointments'])

    return officers_list


test = query_handler('/company/10435750/officers')
officers_list = company_handler(test)
print(officers_list)
# test2 = query_handler('/company/10435750/officers')
# print(test2)


# start with a url. scrape info from url, load into neo4j, find links, repeat.
def crawler_workhorse(starting_url, starting_type=None, to_depth = 5):
    if starting_type is None:
        # try and figure out if starting at company or offier
        split_starting_url = starting_url.split("/")
        if 'appointments' in split_starting_url:
            starting_type = 'officer'
        else:
            starting_type = 'company'

    companies_to_crawl = []
    officers_to_crawl = []
    companies_crawled = []
    officers_crawled = []
    currently_crawling = None

    rate_limit_amount = 600  # 600 requests allowed
    rate_limit_timescale = 300  # every 5 mins

    start_time = datetime.now()
    number_crawled = 0

    data = query_handler(starting_url)

    # if starting type is an officer, run the officer code
    if starting_type == 'officer':
        print("Starting node is an OFFICER. Crawling...")

        companies_to_crawl = officer_handler(data)
        currently_crawling = 'companies'


            # if starting type is a company, run the
    elif starting_type == 'company':
        print("Starting node is a COMPANY. Crawling...")

        officers_to_crawl = company_handler(data)
        currently_crawling = 'officers'


    number_crawled += 1
    depth_crawled = 1

    # as long as there are things to crawl, let's crawl
    while (len(officers_to_crawl) + len(companies_to_crawl)) > 0 and (depth_crawled < to_depth):
        print("Currently crawling: ", currently_crawling)
        print(f"Companies crawled: {len(companies_crawled)}, Companies to crawl: {len(companies_to_crawl)}")
        print(f"Officers crawled: {len(officers_crawled)}, Officers to crawl: {len(officers_to_crawl)}")
        if currently_crawling == 'officers':
            for officer in officers_to_crawl:
                # get data for that officer
                officer_data = query_handler(officer)
                number_crawled += 1
                # pass to officer handler
                new_companies = officer_handler(officer_data)

                for company in new_companies:
                    # ONLY append companies we haven't crawled so we don't end up with infinite recursion
                    if company not in companies_crawled:
                        companies_to_crawl.append(company)

                # mark officer as having been crawled
                officers_crawled.append(officer)

            # now all officers have been crawled, clear the list
            officers_to_crawl = []

            # switch case
            currently_crawling = 'companies'
            # we can only increment the depth crawler on the OFFICER step since we only add nodes here
            depth_crawled += 2

        elif currently_crawling == 'companies':
            for company in companies_to_crawl:
                # get data for that company
                company_data = query_handler(company)
                number_crawled += 1

                # pass to company handler
                new_officers = company_handler(company_data)

                for officer in new_officers:
                    # ONLY append officers we haven't crawled so we don't end up with infinite recursion
                    if officer not in officers_crawled:
                        officers_to_crawl.append(officer)

                # mark company as having been crawled
                companies_crawled.append(company)

            # now all companies have been crawled, clear the list
            companies_to_crawl = []

            # switch case
            currently_crawling = 'officers'




crawler_workhorse(starting_url = '/officers/UNUnLctDoR4WsYnYTFaHbvIHhW8/appointments', to_depth=4)

if purge_at_end:
    purge_db()


# deallocate resources
session.close()
driver.close()
