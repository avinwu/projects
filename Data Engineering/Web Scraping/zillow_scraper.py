import requests,sys
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import numpy as np
import time
from lxml import html
from tqdm import tqdm
from datetime import datetime


def get_zillow_listings(pid, nIdx, url, file_dir):
    now = datetime.now()  # current date and time
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    out_file_bldg = file_dir + 'zillow_listings_dtl_out_blgds_1.csv'
    out_file_othr = file_dir + 'zillow_listings_dtl_out_othrs_1.csv'

    req_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Connection': 'close',
        'Host': 'www.zillow.com',
        'Accept': 'text/html, application/xhtml+xml, application/xml; q=0.9, */*; q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.8',
        'upgrade-insecure-requests': '1',
        'Cache-Control': 'max-age=0',
        'Cookie': 'AWSALB=update_your_cookie_here',
        'user-agent': 'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }
    req_headers['Referer'] = url
    # print(response.status_code)

    try:
        response = requests.get(url, headers=req_headers)
        # print(response.status_code)
        resp_code = response.status_code
        resp_text = response.text

        if 'name="robots"' in resp_text:
            print(f"[DEBUG] CAPTCHA")
            sys.exit()

        soup = BeautifulSoup(resp_text, 'html.parser')
        # print(soup.title)

        script = soup.find('script', {'id': '__NEXT_DATA__',
                                      'type': 'application/json'}).text
        dict_1 = json.loads(script)
        sPage = dict_1['page'][1:]  # get rid of the leading "/"

        if sPage == 'building':
            # Get Listing Details for buildings
            get_lstng_dtl_bldng(pid, nIdx, url, out_file_bldg, dict_1, date_time)
        else:
            # Get Listing Details for others
            get_lstng_dtl_other(pid, nIdx, url, out_file_othr, dict_1, date_time)

    except Exception as e:
        print(f"[FAILURE] [{date_time}] PID# {nIdx}: {url} [ERROR] {e}")
        return

    # debug print
    # for k in dctPdtl: print("{0:25} {1}".format(k, dctPdtl[k]))

def get_lstng_dtl_bldng(pid, nIdx, url, out_file, dict_1, date_time):
    dctPdtl = {}
    dctPdtl['pid'] = pid

    dict_2 = dict_1['props']['pageProps']['initialData']['building']

    lstKeys = ['zpid', '__typename', 'buildingType', 'streetAddress', 'latitude', 'longitude', 'city', 'state',
               'zipcode',
               'county', 'buildingName', 'buildingPhoneNumber', 'buildingAttributes', 'amenitySummary', 'homeTypes']

    for l in lstKeys:
        if l in dict_2.keys():
            dctPdtl[l] = dict_2[l]
        else:
            dctPdtl[l] = ''

    lstUnits = dict_2['ungroupedUnits']

    # lstUnitPID, lstUnitNum, lstUnitURL, lstUnitTyp, lstUnitLstTyp = [], [], [], [], []
    lstUnitPID, lstUnitURL = [], []

    for u in lstUnits:
        lstUnitPID.append(u['zpid'])
        # lstUnitNum.append(u['unitNumber'])
        lstUnitURL.append(u['hdpUrl'])
        # lstUnitTyp.append(u['__typename'])
        # lstUnitLstTyp.append(u['listingType'])

    dctPdtl['unit_pid'] = lstUnitPID
    # dctPdtl['unit_num'] = lstUnitNum
    dctPdtl['unit_url'] = lstUnitURL
    # dctPdtl['unit_typ'] = lstUnitTyp
    # dctPdtl['unit_lst_typ'] = lstUnitLstTyp

    row = list(dctPdtl.values())
    df = pd.DataFrame(row).T
    df.to_csv(out_file, mode='a', index=False, header=False)

    print(f"[SUCCESS] [{date_time}] PID# {nIdx}: {url}")


def get_lstng_dtl_other(pid, nIdx, url, out_file, dict_1, date_time):
    # Dictionary to hold all the fields to be extracted for this pid.
    dctPdtl = {}
    dctPdtl['pid'] = pid

    s = dict_1['props']['pageProps']['gdpClientCache']
    dict_2 = json.loads(s)
    key_dict_2 = list(dict_2.keys())[0]
    dict_3 = dict_2[key_dict_2]
    dict_4 = dict_3['property']

    lstKeys = \
        ['zpid', 'homeType', 'streetAddress', 'city', 'state', 'zipcode', 'latitude', 'longitude', 'county', 'country',
         'parcelId', 'bedrooms', 'bathrooms', 'zestimate', 'rentZestimate', 'yearBuilt', 'livingArea',
         'livingAreaValue',
         'livingAreaUnitsShort', 'lotSize', 'lotAreaValue', 'lotAreaUnits', 'currency', 'taxAssessedValue',
         'taxAssessedYear',
         'monthlyHoaFee', 'propertyTaxRate', 'lastSoldPrice', 'dateSoldString', 'parentRegion', 'neighborhoodRegion',
         'building', 'boroughId', 'providerListingID', 'hdpUrl']

    for l in lstKeys:
        if l in dict_4.keys():
            dctPdtl[l] = dict_4[l]
        else:
            dctPdtl[l] = ''

    lstKeys = \
        ['appliances', 'heating', 'cooling', 'fireplaceFeatures', 'fireplaces', 'flooring', 'levels', 'stories',
         'storiesTotal', 'ownershipType', 'parkingCapacity', 'parkingFeatures', 'otherParking', 'roofType', 'rooms',
         'propertyCondition', 'constructionMaterials', 'exteriorFeatures', 'architecturalStyle', 'waterView',
         'waterViewYN', 'windowFeatures', 'hasAdditionalParcels', 'hasPetsAllowed', 'hasRentControl', 'hasHomeWarranty',
         'isNewConstruction', 'hasAssociation', 'hasAttachedGarage', 'hasAttachedProperty', 'hasCooling', 'hasCarport',
         'hasElectricOnProperty', 'hasFireplace', 'hasGarage', 'hasHeating', 'hasLandLease', 'hasOpenParking',
         'hasSpa', 'hasPrivatePool', 'hasView', 'hasWaterfrontView', 'elementarySchool', 'elementarySchoolDistrict']

    dict_5 = dict_4['resoFacts']

    for l in lstKeys:
        if l in dict_5.keys():
            dctPdtl[l] = dict_5[l]
        else:
            dctPdtl[l] = ''

    row = list(dctPdtl.values())
    df = pd.DataFrame(row).T
    df.to_csv(out_file, mode='a', index=False, header=False)

    print(f"[SUCCESS] [{date_time}] PID# {nIdx}: {url}")

# main call

if __name__ == "__main__":

    inp_dir, out_dir = '../data_inp/', '../data_out/'
    inp_file = inp_dir + 'zillow_listings_url_inp_1.csv'
    #out_file = out_dir + 'zillow_listings_dtl_out.csv'
    nSleep=5

    df = pd.read_csv(inp_file)
    dct = df.to_dict('index')
    #lstKeys = df.to_dict('index').keys()

    for nIdx in dct.keys():
        #nURLIdx += 1
        pid = dct[nIdx]['property_id']
        url = dct[nIdx]['url']

        get_zillow_listings(pid, nIdx, url, out_dir)

        # nSleep += 2
        # if nSleep>=60: nSleep=10 #Reset sleep time back to 10seconds
        time.sleep(nSleep)