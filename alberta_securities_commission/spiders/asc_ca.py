from scrapy.cmdline import execute
from datetime import datetime
import browserforge.headers
from typing import Iterable
from scrapy import Request
import pandas as pd
import unicodedata
import random
import scrapy
import time
import json
import evpn
import os
import re


def remove_punctuation(text):
    if text == 'N/A':
        return text
    return ''.join(char for char in text if not (unicodedata.category(char).startswith('P') and char != '|'))


def replace_with_na(text: str) -> str:
    return re.sub(pattern=r'^[\s_-]+$', repl='N/A', string=text)  # Replace _, __, -, --, --- with N/A


# Function to remove Extra Spaces from Text
def remove_extra_spaces(_text: str) -> str:
    return ' '.join(_text.split())  # Remove extra spaces


def df_cleaner(data_frame) -> pd.DataFrame:
    columns = data_frame.columns.sort_values()
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame
    # Apply the function to all columns for Cleaning
    for column in columns:
        # if 'title' in column or 'parties_involved' in column or 'alias' in column:
        if any(keyword in column for keyword in ['title', 'parties_involved', 'alias']):
            # Remove punctuation
            data_frame[column] = data_frame[column].str.replace('–', '')
            data_frame[column] = data_frame[column].apply(remove_punctuation)
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces
        data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
    priority_columns = ["url", "title", "date", "type", "pdf_url"]
    columns_required = priority_columns + [col for col in columns if col not in priority_columns]
    data_frame = data_frame[columns_required]
    data_frame.replace(to_replace=r'^\s*$', value=None, regex=True, inplace=True)
    data_frame.replace('NA', pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    return data_frame


def remove_diacritics(input_str):
    return ''.join(char for char in unicodedata.normalize('NFD', input_str) if not unicodedata.combining(char))


def get_pdf_url(result_dict: dict) -> str:
    click_uri: str = result_dict.get('clickUri', 'N/A')  # Extract the clickUri
    pdf_base_url: str = 'https://www.asc.ca'  # Convert the clickUri to the desired format
    pdf_url: str = click_uri.replace('https://asc-cws-prod-web-cm-staging.azurewebsites.net', pdf_base_url)  # Replace the base URL with the desired base URL
    return pdf_url if click_uri not in ['', ' ', None] else 'N/A'


def get_title_alias(result_dict: dict) -> tuple:
    titles_list: list = result_dict.get('raw', {}).get('z95xsitecoretitle', ['N/A'])
    cleaned_titles_list: list = [titles.replace("\n", "").replace("<br>", "") for titles in titles_list]  # Remove newline character '\n' and remove '<br>'
    alias_keywords = ['formerly known as', 'carrying on business as', 'previously known as', 'now known as',
                      'also known as', 'operating as', 'formerly', 'known as', 'a.k.a.', 'aka', 'dba', 'Inc', 'Ltd', 'Inc.', 'Ltd.', '.,', ';']
    title_value_list, alias_value_list = [], []
    # Regex pattern to remove punctuation
    for title_index, title in enumerate(cleaned_titles_list):
        for alias_keyword in alias_keywords:
            title = ' | '.join(title.split(alias_keyword))
        splitted_title = title.split('|')
        _title_value = splitted_title[0].strip() if len(splitted_title) > 1 else ' '.join(splitted_title).strip()
        _alias_value = ' '.join(splitted_title[1:]).strip('.').strip() if len(splitted_title) > 1 else 'N/A'
        title_value = _title_value if _title_value not in ['', None, ' .', ' ', '.'] else 'N/A'
        if title_value not in ['N/A', '']:
            title_value_list.append(title_value)
        alias_value = _alias_value if _alias_value not in ['', None, ' .', ' ', '.'] else 'N/A'
        if alias_value not in ['N/A', '']:
            alias_value = alias_value.strip('.').strip()
            alias_value_list.append(alias_value)

    title_value_list = [value for value in title_value_list if value != '']
    alias_value_list = [value for value in alias_value_list if value != '']
    title_value_str = ' | '.join(title_value_list).strip() if title_value_list != [] else 'N/A'
    alias_value_str = ' | '.join(alias_value_list).strip() if alias_value_list != [] else 'N/A'
    return title_value_str, alias_value_str


def get_parties_involved(result_dict: dict) -> str:
    parties_involved: str = ' | '.join(result_dict.get('raw', {}).get('z95xpartiesinvolved', ['N/A'])).strip()
    parties_involved = parties_involved if parties_involved not in ['', None] else 'N/A'
    return parties_involved


def get_date(result_dict: dict) -> str:
    # Assuming sysdate is in milliseconds (as it seems to be a UNIX timestamp in ms)
    sysdate = result_dict.get('raw', {}).get('sysdate')
    date = 'N/A'
    if sysdate not in ['', ' ', None, []]:
        sysdate_seconds = sysdate / 1000  # Convert milliseconds to seconds (Python's datetime works with seconds)
        item_date = datetime.fromtimestamp(sysdate_seconds)  # Convert the timestamp to a datetime object
        date = item_date.strftime("%Y-%m-%d")  # Format the date to "YYYY-MM-DD" similar to the JavaScript output
    return date


def get_notices_type(result_dict: dict) -> str:
    notices_type_list: str = ' | '.join(result_dict.get('raw', {}).get('z95xnoticesdecisionstype', ['N/A'])).strip()
    notices_type = notices_type_list if notices_type_list not in ['', None] else 'N/A'
    return notices_type


class AscCaSpider(scrapy.Spider):
    name = "asc_ca"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('Connecting to VPN (CANADA)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (CANADA)
        self.api.connect(country_id='181')  # canada country code
        time.sleep(10)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.final_data = list()
        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.cookies = {
            '_gcl_au': '1.1.1734506661.1729499227',
            '_ga': 'GA1.1.1717590813.1729499227',
            '_fbp': 'fb.1.1729499227312.179531952380438286',
            'sa-user-id': 's%253A0-e9f31ab4-8efe-58d7-4b12-989d4efcbe73.I%252BJOQWzaUScgtmTy38MDWiH1ZFHQCCmkpAyo%252BEiOiNI',
            'sa-user-id-v2': 's%253A6fMatI7-WNdLEpidTvy-cxttCmo.jp3yAphoBCPW6ZEXiw%252BzIpqwjk1SeGUmcj6jH30D%252B8M',
            'sa-user-id-v3': 's%253AAQAKINb7zoloiGfr2p8m3dq7GizxPzQHzlE9Ka-iIB0vp-16EHwYBCDt7fS3BjABOgSiCRyuQgTmHXCz.hfXbI1Gy%252FeViEhy%252FkpmXihC4tgjAfJcoTC4C58xwX1k',
            '_ga_L2NZ0358YT': 'GS1.1.1729499227.1.1.1729501235.48.0.0',
            '_ga_MP2P11677J': 'GS1.1.1729499227.1.1.1729501235.0.0.0',
        }

        # self.headers = {
        #     'accept': '*/*',
        #     'accept-language': 'en-US,en;q=0.9',
        #     'authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJ2OCI6dHJ1ZSwidG9rZW5JZCI6InFpcTQ0ZmwyYmtpMmVtbTJ3czVwcXcyYWptIiwib3JnYW5pemF0aW9uIjoiYWxiZXJ0YXNlY3VyaXRpZXNjb21taXNzaW9ucHJvZDJhMzR0aXk3IiwidXNlcklkcyI6W3sidHlwZSI6IlVzZXIiLCJuYW1lIjoiZXh0cmFuZXRcXEFub255bW91cyIsInByb3ZpZGVyIjoiRXhwYW5kZWQgU2l0ZWNvcmUgU2VjdXJpdHkgUHJvdmlkZXIgZm9yIEFTQy1QUk9EIn0seyJ0eXBlIjoiVXNlciIsIm5hbWUiOiJhbm9ueW1vdXMiLCJwcm92aWRlciI6IkVtYWlsIFNlY3VyaXR5IFByb3ZpZGVyIn1dLCJyb2xlcyI6WyJxdWVyeUV4ZWN1dG9yIl0sImlzcyI6IlNlYXJjaEFwaSIsImV4cCI6MTcyOTU4NDQwOSwiaWF0IjoxNzI5NDk4MDA5fQ.L2zvXVicznIAlVhQwEW_M-Jb2COaGu69JeTsrsXtSfA',
        #     'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        #     'origin': 'https://www.asc.ca',
        #     'priority': 'u=1, i',
        #     'referer': 'https://www.asc.ca/en/enforcement/notices-decisions-and-orders',
        #     'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        #     'sec-ch-ua-mobile': '?0',
        #     'sec-ch-ua-platform': '"Windows"',
        #     'sec-fetch-dest': 'empty',
        #     'sec-fetch-mode': 'cors',
        #     'sec-fetch-site': 'same-origin',
        #     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        # }
        # Headers changes at some interval, hence using HeaderGenerator to generate headers
        self.headers = browserforge.headers.HeaderGenerator().generate()
        self.number_of_results = 10  # Results per page
        self.first_result = 0  # Start from the first

        self.data = f'actionsHistory=%5B%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-10-21T09%3A00%3A23.762Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-10-21T08%3A29%3A47.115Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-10-21T08%3A27%3A07.652Z%5C%22%22%7D%5D&referrer=&analytics=%7B%22clientId%22%3A%220b8e43c0-c182-0f40-c2b9-26bbcc7a8920%22%2C%22documentLocation%22%3A%22https%3A%2F%2Fwww.asc.ca%2Fen%2Fenforcement%2Fnotices-decisionand-orders%23sort%3D%2540z95xcreateddate%2520descending%22%2C%22documentReferrer%22%3A%22%22%2C%22pageId%22%3A%22%22%7D&visitorId=0b8e43c0-c182-0f40-c2b9-26bbcc7a8920&isGuestUser=false&aq=NOT%20%40z95xtemplate%3D%3D(ADB6CA4F03EF4F47B9AC9CE2BA53FF97%2CFE5DD82648C6436DB87A7C4210C7413B)&cq=(%40z95xlanguage%3D%3Den)%20(%40z95xlatestversion%3D%3D1)%20(%40source%3D%3D%22Coveo_public_index%20-%20ASC-PROD%22)&searchHub=Notices%20Decisions%20and%20Orders&locale=en&pipeline=noticesdecisionsordersenforcement&maximumAge=900000&firstResult={self.first_result}&numberOfResults=10&excerptLength=200&enableDidYouMean=false&sortCriteria=%40z95xcreateddate%20descending&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40z95xnoticesdecisionstype%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40z95xcreateddateyear%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22alphaDescending%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=Asia%2FCalcutta&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&allowQueriesWithoutKeywords=true'

        self.browsers = ["chrome110", "edge99", "safari15_5"]

        self.url = 'https://www.asc.ca/coveo/rest/search/v2?sitecoreItemUri=sitecore%3A%2F%2Fweb%2F%7B4914B9E4-A101-438A-A8DF-C04C42874916%7D%3Flang%3Den%26ver%3D2&siteName=asc'
        self.onsite_page_url = 'https://www.asc.ca/en/enforcement/notices-decisions-and-orders#first={SKIP_COUNT}&sort=%40z95xcreateddate%20descending'

    def start_requests(self) -> Iterable[Request]:

        yield scrapy.Request(url=self.url, cookies=self.cookies, headers=self.headers, method='POST', meta={'impersonate': random.choice(self.browsers)}, callback=self.parse, body=self.data, dont_filter=True)

    def parse(self, response, **kwargs):
        response_dict = json.loads(response.text)
        total_count = response_dict.get('totalCountFiltered', 0)

        # Process current page data
        self.process_page_data(response_dict)

        # Pagination logic: Keep requesting until the last page is reached
        if self.first_result < total_count:
            # Update 'self.data' to modify `firstResult` for the next page
            self.first_result += self.number_of_results
            self.data = self.data.replace(f'&firstResult={self.first_result - self.number_of_results}', f'&firstResult={self.first_result}')

            # Send request for the next page
            yield scrapy.Request(url=self.url, method='POST', body=self.data, callback=self.parse, headers=self.headers, cookies=self.cookies, dont_filter=True)

    def process_page_data(self, response_dict):
        # Process each page's results
        for result_dict in response_dict.get('results', []):
            data_dict = dict()
            data_dict['url'] = self.onsite_page_url.replace('first={SKIP_COUNT}', f'first={self.first_result}')
            data_dict['pdf_url'] = get_pdf_url(result_dict)
            data_dict['date'] = get_date(result_dict)
            title_alias_tuple = get_title_alias(result_dict)
            data_dict['title'] = title_alias_tuple[0]
            data_dict['alias'] = title_alias_tuple[1]
            data_dict['type'] = get_notices_type(result_dict)
            data_dict['parties_involved'] = get_parties_involved(result_dict)
            self.final_data.append(data_dict)
            # print(data_dict)

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            data_df = pd.DataFrame(self.final_data)
            data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
            with pd.ExcelWriter(path=self.filename, engine='xlsxwriter') as writer:
                data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
                data_df.to_excel(excel_writer=writer, index=False)
            print("Native Excel file Successfully created.")
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()
            print('VPN Connected!' if self.api.is_connected else 'VPN Disconnected!')


if __name__ == '__main__':
    execute(f'scrapy crawl {AscCaSpider.name}'.split())
