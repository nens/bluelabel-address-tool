# -*- coding: utf-8 -*-
"""
@author: Emile.deBadts
Nelen & Schuurmans 
"""
import getpass
import requests
import pandas as pd
import datetime
import os
import re
import logging

VALID_AT = datetime.datetime.now().replace(microsecond=0).isoformat()
LIZARD_LABEL_URL = r"https://bluelabel.lizard.net/api/v3/labels/"
LIZARD_BUILDINGS_URL = r"https://bluelabel.lizard.net/api/v3/buildings/"
LOGIN_URL = "https://bluelabel.lizard.net/api-auth/login/"
USER_AGENT = "Nelen-Schuurmans/labelextract"
RETRY_ATTEMPTS = 3

logging.basicConfig(filename='output/logging.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level = logging.INFO)

def parse_huisnummer(huisnummer):

    huisnummer = str(huisnummer).replace(" ", "")

    if "-" in huisnummer:
        huisnummer = huisnummer.split("-")[0]

    re_letter = re.search("[a-zA-Z]", huisnummer)

    if bool(re_letter):
        huisnummer_split = re.split("(\d+)", huisnummer)
        huisnummer = huisnummer_split[1]
        huisletter = huisnummer_split[2].upper()
    else:
        huisletter = None

    return (huisnummer, huisletter)


def get_session(username):

    # login
    password = getpass.getpass("Password for %s: " % username)
    session = requests.Session()

    r = session.post(
        url=LOGIN_URL,
        headers={"User-Agent": USER_AGENT},
        data={"username": username, "password": password},
    )

    if "sessionid" not in session.cookies:
        print("Login failed")
        logging.error("Login failed")
        exit()
    else:
        return session

def main():

    label_uuid_dict = {
        "flooding": "a84890f3-37de-4073-ad96-19c243f87b93",
        "heatstress": "57dd670c-b23a-437e-a336-5e295da59cba",
        "drought": "8e979623-4022-4dbc-96f7-9492d2c84b8b",
        "pluvial": "11117a90-b1ef-48cf-8a4f-aa086d1457f4",
    }
    
    username = input('Username: ')
    session = get_session(username)
    
    try:
        input_df = pd.read_excel('input/input.xlsx')
    except Exception as e:
        logging.error("Unable to open excel file")
        print("Can't open input file, is the input file named 'input.xlsx'?", e)
        exit()

    input_df.columns = [column.lower() for column in input_df.columns]

    if "huisnummer" not in input_df.columns or "postcode" not in input_df.columns:
        logging.error("Missing column 'huisnummer' or 'postcode'")
        print("Missing column in input file (columns huisnummer/postcode needed)")
        exit()

    output_columns = [*list(input_df.columns), *list(label_uuid_dict.keys())]
    output_df = pd.DataFrame(columns=output_columns)

    for i in input_df.index:
        
        huisnummer = input_df.loc[i]["huisnummer"]
        postcode = input_df.loc[i]["postcode"]
        
        print("Extracting labels for ", huisnummer, ", ", postcode)

        if pd.isnull(huisnummer) or pd.isnull(postcode):
            append_dict = {
                **input_df.iloc[i].to_dict(),
                **{key: [None] for key in label_uuid_dict},
            }
            logging.warning("Missing huisnummer or postcode for entry with index {}".format(i))
            output_df = output_df.append(pd.DataFrame(append_dict), sort=False)

        else:

            postcode = str(postcode).replace(" ", "")
            huisnummer, huisletter = parse_huisnummer(huisnummer)

            params = {
                "addresses__postalcode": postcode,
                "addresses__house_number": huisnummer,
                "addresses__house_letter": huisletter,
                "addresses__house_number_suffix": None,
                "format": "json",
                "valid_at": None
            }

            building_response = session.get(LIZARD_BUILDINGS_URL, params=params)
            building_content = building_response.json()
            building_results = building_content['results']
            # Reverse the builings list as to always start with the most recently updated buildings
            building_results.reverse()
            
            append_dict = {
                    **input_df.iloc[i].to_dict(),
                    **{key: [None] for key in label_uuid_dict.keys()},
                }

            for j in range(0,len(building_results)):

                building_id = building_content["results"][j]["id"]

                for labeltype in label_uuid_dict.keys():

                    label_uuid = label_uuid_dict[labeltype]
                    
                    if append_dict[labeltype] == [None]:  
                        
                        params_labels = {
                            "object_id": building_id,
                            "label_type__uuid": label_uuid,
                            "valid_at": VALID_AT
                        }
                        
                        retry_count = 1
                        response_bool = False
                        while not response_bool and retry_count <= RETRY_ATTEMPTS:
    
                            label_response = session.get(LIZARD_LABEL_URL, params=params_labels)
                            response_bool = label_response.ok
                            label_content = label_response.json()
        
                            if len(label_content["results"]) > 0:
                                append_dict[labeltype] = [label_content["results"][0]["label_value"]]
                                logging.info("Label {labeltype} extracted for {huisnummer}, {postcode} using building {building_id}".format(labeltype=labeltype,huisnummer=huisnummer,postcode=postcode, building_id=building_id))
                               
                                if building_content['results'][j]['end'] is not None:
                                    logging.warning("Label was extracted with deprecated building (building_id={building_id})".format(building_id=building_id))
                            else:
                                retry_count += 1

            output_df = output_df.append(pd.DataFrame(append_dict), sort=False)

    session = None
    if not os.path.exists('output'):
        os.mkdir('output')
    if os.path.exists("output/output.xlsx"):
        os.remove("output/output.xlsx")
    output_df.to_excel('output/output.xlsx', index=False)


if __name__ == "__main__":
    exit(main())
