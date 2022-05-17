import requests
from bs4 import BeautifulSoup

SEC_FORM_URL = 'https://www.sec.gov/forms'

if __name__ == '__main__':
    soup = BeautifulSoup(requests.get(SEC_FORM_URL).text, 'lxml')

    table = soup.find('table')

    def clean(string):
        return string.split(':')[1].strip().replace('\n', '')

    with open('form_types.txt', 'w+') as form_type_file:
        for row in table.find_all('tr')[1:]:
            try:
                id = row.find(attrs={'headers': 'view-field-release-number-table-column'}).text
                desc = row.find(attrs={'headers': 'view-field-display-title-table-column'}).text

                id = clean(id)
                desc = clean(desc)

                if id in ('n/a', ):
                    continue

                # last_updated = row.find(attrs={'headers': 'view-field-date-table-column'})
                print(id, desc)
                form_type_file.write(f"{id} = '{desc}'\n")
            except UnicodeEncodeError:
                continue
