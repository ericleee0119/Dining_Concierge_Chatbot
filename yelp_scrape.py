from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib
import boto3
import decimal
import pandas as pd

from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode


API_KEY= 'AHXa5siHp7V26CcwO5JbLphHGilSUsIWbt6Kdtx4gdnpCXQ6ERqZ5OGP87ssxdV7ImrIW7vowQeX4Ei3FYUeaSguhjHXvFNZDEaZ3d2TaHv6M6WQj54QEr2D8hBWYXYx' 

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'


def request(host, path, api_key, url_params=None):
    url = f"{host}{quote(path.encode('utf8'))}"
    headers = {'Authorization': 'Bearer %s' % api_key}
    print(u'Querying {0} ...'.format(url))
    response = requests.request('GET', url, headers=headers, params=url_params)
    return response.json()


def search(api_key, term, location, offSet):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
         'offset': offSet,
         'limit': 50    
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def query_api():
    buid = []
    name = []
    addr = []
    cord = []
    norv = []
    rate = []
    zipc = []
    cuis = []
    res = []

    id_set = set()

    cuisines = ['chinese', 'american', 'japanese', 'indian', 'italian']
    location = ['Manhattan', 'Brooklyn', 'Flushing', 'Bronx', 'Queens']
    
    for cuisine in cuisines:
        cuisine_cnt = 0
        for l in location:
            offset = 0
            business = []
            while cuisine_cnt < 1000:
                response = search(API_KEY, cuisine+' restaurants', l, offset)
                get_busi = response.get('businesses')

                if not get_busi:
                    print(f"No businesses for {cuisine} restaurants in {l} found.")
                    break
                
                business.append(get_busi)
                offset += 50
                cuisine_cnt += 50

            for busi in business:
                if not busi:
                    continue
                dct = {}
                for b in busi:
                    if not b:
                        continue
                    if b['id'] in id_set:
                        continue

                    if (b['location']['zip_code']):
                        z = b['location']['zip_code']
                    else:
                        z = None

                    if (b['coordinates'] and b['coordinates']['latitude'] and b['coordinates']['longitude']):
                        c = f"{b['coordinates']['latitude']}, {b['coordinates']['longitude']}"
                    else:
                        c = None

                    buid.append(b['id'])
                    name.append(b['name'])
                    addr.append(', '.join(b['location']['display_address']))
                    cord.append(c)
                    norv.append(int(b['review_count']))
                    rate.append(float(b['rating']))
                    zipc.append(z)
                    cuis.append(cuisine)

                    dct["uuid"] = b['id']
                    dct["name"] = b['name']
                    dct["address"] = ', '.join(b['location']['display_address'])
                    dct["coordinate"] = c
                    dct["reviews"] = int(b['review_count'])
                    dct["Rating"] = float(b['rating'])
                    dct["zipcode"] = z
                    dct["cuisine"] = cuisine

                    res.append(dct)
                    id_set.add(b['id'])
                print(f"{len(busi)} {cuisine} restaurants in {l} just added")

        print(f"Finish adding {cuisine} restaurants")

    df = pd.DataFrame(
        data={
                "uuid": buid,
                "Name": name,
                "Address":addr,
                "Coordinate":cord,
                "Reviews":norv,
                "Rating":rate,
                "Zipcode":zipc,
                "Cuisine":cuis
        }
    )
    df.to_csv("./test.csv", index=False)
    with open('data.json', 'w') as openfile:
        json.dump(res, openfile, indent = 4)



def main():
    try:
        query_api()
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


if __name__ == '__main__':
    main()