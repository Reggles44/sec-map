from bs4 import BeautifulSoup
from xbrlassembler import XBRLType, XBRLAssembler

from sec_map.utils import get


SEC_INDEX_URL = 'https://www.sec.gov/Archives/edgar/data/{}/{}-index.htm'


async def make_assembler(cik, index_id) -> XBRLAssembler:
    index_response = await get(SEC_INDEX_URL.format(cik, index_id))
    if not index_response:
        return

    index_soup = BeautifulSoup(index_response.content, 'lxml')
    data_files_table = index_soup.find('table', {'summary': 'Data Files'})
    if not data_files_table:
        return

    file_map = {}

    for row in data_files_table('tr')[1:]:
        link = "https://www.sec.gov" + row.find_all('td')[2].find('a')['href']
        file_name = link.rsplit('/', 1)[1]
        xbrl_type = XBRLType(file_name)
        if xbrl_type:
            file_map[xbrl_type] = BeautifulSoup(await get(link), 'lxml')

    return XBRLAssembler.parse(file_map, ref_doc=XBRLType.PRE)
3