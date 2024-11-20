import datetime
import json
import re
import requests
from bs4 import BeautifulSoup
import utils
from prefect import flow


BASE_URL = 'https://enco.ru'


@flow(log_prints=True)
def parse():
    utils.upload(parse_core())


def parse_core():
    return {
        'systemName': 'ЭНКО',
        'name': 'ИНВЕСТИЦИОННАЯ СТРОИТЕЛЬНАЯ КОМПАНИЯ "ЭНКО"',
        'residentialComplexes': list(parse_complexes())
    }


def get_soup(url):
    response = requests.get(url)
    return BeautifulSoup(response.text, 'lxml')


def get_text(tag):
    return tag.text.strip() if tag else ''


def parse_complexes():
    response = requests.get('https://nur.enco.ru/projects/')
    string = re.search(r"var projectsObjects = (.+?)</script>",
                       response.text, re.DOTALL).group(1).strip()
    quote_keys = lambda text: re.sub(r'\b(\w+):', r"'\1':", text)
    string = quote_keys(string).replace('\'', '"')
    remove_commas = lambda text: re.sub(r',\s*\n*\s*([}[\]])', r'\1', text)
    string = remove_commas(string)
    complexes = json.loads(string)['items']

    for complex in complexes:
        slug = complex['link'].split('/')[2]
        yield {
            'internalId': slug,
            'name': complex['name'],
            'geoLocation': {
                'latitude': complex['center'][0],
                'longitude': complex['center'][1],
            },
            'renderImageUrl': BASE_URL + complex['menuImgPath'],
            'presentationUrl': None,
            'flats': list(parse_flats(slug)),
        }
    print('Complexes are parsed')


def evaluate_building_deadline(string):
    if string == 'Дом сдан' or string == 'Вторичная':
        return datetime.date.min.isoformat()
    if string == 'Сдаем в этом году':
        quarter, year = 4, int(datetime.datetime.now().year)
    else:
        quarter, year = int(string.split()[2]), int(string.split()[-1])
    return utils.create_date_from_quarter(year, quarter).isoformat()


def parse_flats(slug):
    soup = get_soup(f'https://enco.ru/search/apartments/project/{slug}')
    flats_total = int(get_text(soup.select_one('b.page-filters__result')).split()[0])
    pages = 1 + (flats_total - 1) // 12

    for page in range(1, pages + 1):
        soup = get_soup(f'https://enco.ru/search/apartments/project/{slug}/?PAGEN_1={page}') \
            if page > 1 else soup

        flat_links = [BASE_URL + tag.select_one('a.product-card__link')['href']
                      for tag in soup.select('div._tile')]

        for flat_link in flat_links:
            print(flat_link)
            soup = get_soup(flat_link)
            parts = get_text(soup.select_one('div.product-info-card__title')).split(',')
            area, string = parts[1].strip(), parts[0].strip()
            rooms = 0 if string == 'Студия' else int(string.split('-')[0])
            print(list(set(soup.select('span.product-feature'))))
            string = get_text(list(dict.fromkeys(soup.select('span.product-feature')))[-1])
            image = BASE_URL + soup.select_one("img[itemprop='image']")['src']
            yield {
                'residentialComplexInternalId': slug,
                'developerUrl': flat_link,
                'price': get_text(soup.select_one('span.js-current-price')),
                'floor': int(get_text(soup.h1).split('этаж')[0].split(',')[-1].strip()),
                'area': area,
                'rooms': rooms,
                'buildingDeadline': evaluate_building_deadline(string),
                'layoutImageUrl': image,
            }
        print(f'Сomplex \'{slug}\' flats on page {page}/{pages} parsed')


if __name__ == '__main__':
    print(json.dumps(parse_core()))
