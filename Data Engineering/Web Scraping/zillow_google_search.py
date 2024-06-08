
from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime
import pandas as pd


def get_search_result(nIdx, sPID, sAddr):
    now = datetime.now()  # current date and time
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    url = 'https://www.google.com/search'
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'accept-encoding': 'gzip,deflate,br',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    }
    headers['Referer'] = sAddr

    try:
        parameters = {'q': sAddr}
        response = requests.get(url, headers=headers, params = parameters)

        resp_code = response.status_code
        resp_text = response.text
        #print(resp_code, resp_text)

        if 'name="robots"' in resp_text:
            print(f"[DEBUG] CAPTCHA")
            sys.exit()

        #print(f"[DEBUG] {resp_code} {resp_text}")

        soup = BeautifulSoup(resp_text, 'html.parser')
        search = soup.find(id = 'search')
        if search:
            first_link = search.find('a')
            sFirstLink = first_link['href']
        else:
            sFirstLink = 'err'

        sFileOut = sPID + "," + sFirstLink + "\n"
        f = open(out_file, "a")
        f.write(sFileOut)
        f.close()

        print(f"[SUCCESS] [{date_time}] PID# {nIdx}: {sAddr}")

    except Exception as e:
        print(f"[FAILURE] [{date_time}] PID# {nIdx}: {sAddr} [ERROR] {e}")

if __name__ == "__main__":

    inp_dir, out_dir = '../data_inp/', '../data_out/'
    inp_file = inp_dir + 'google_zillow_addrs_search_inp_1.csv'
    out_file = out_dir + 'google_zillow_addrs_search_out_1.csv'
    nSleep=5

    df = pd.read_csv(inp_file)
    dct = df.to_dict('index')

    for nIdx in dct.keys():
        #nURLIdx += 1
        sPID  = dct[nIdx]['property_id']
        sAddr = dct[nIdx]['address_str']

        get_search_result(nIdx, sPID, sAddr)

        # nSleep += 2
        # if nSleep>=60: nSleep=10 #Reset sleep time back to 10seconds
        time.sleep(nSleep)