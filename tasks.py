from robocorp.tasks import task
from robocorp import workitems
from RPA.Excel.Files import Files
from datetime import datetime
import dateutil.relativedelta
from urllib import parse
import requests
import json
import os
import re
import logging
import threading
import traceback

REQUEST_PAGE_SIZE = 30

@task
def main_task():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s %(lineno)d| %(message)s')
    logging.info('===================================== TASK STARTED =====================================')
    workitem = workitems.inputs.current

    try:
        keyword = workitem.payload['keyword']
        section = workitem.payload['section']
        months = workitem.payload['months']

        pages = make_request(keyword, section, months)
        infos = []
        for index, page in enumerate(pages):
            logging.info(f'Reading page: {index}')
            infos+=read_articles(keyword,page['result'])
            logging.info(f'Finished page: {index}')
        path = create_result_file(infos)

        workitem.payload['status'] = 'completed'
        workitem.payload['articles'] = len(infos)
        workitems.outputs.create(payload=workitem.payload)
        workitem.done()

    except StepException as step_e:
        logging.error('Error code: '+step_e.code+', message:'+step_e.message)
        workitem.payload['status'] = 'error'
        workitem.payload['error_code'] = step_e.code
        workitem.payload['message'] = step_e.message
        workitems.outputs.create(payload=workitem.payload)
        workitem.fail('BUSINESS', step_e.code, step_e.message)
    except Exception as exception:
        logging.error(exception)
        error_message = traceback.format_exc().splitlines()[-1]
        workitem.payload['status'] = 'error'
        workitem.payload['error_code'] = 'UNEXPECTED_ERROR'
        workitem.payload['message'] = error_message
        workitems.outputs.create(payload=workitem.payload)
        workitem.fail('APPLICATION', 'UNEXPECTED_ERROR', error_message)

    logging.info('===================================== TASK COMPLETED =====================================')

def read_articles(keyword, result):
    if('articles' in result):
        threads = [None] * len(result['articles'])
        infos = [None] * len(result['articles'])
        i = 0
        while(i < len(result['articles'])):
            threads[i] = threading.Thread(target=extract, args=(result['articles'][i],keyword, infos,i))
            threads[i].start()
            i += 1

        for i in range(len(threads)):
            threads[i].join()

        return infos
    else:
        return []

def extract(article, keyword, infos, i):
    logging.info(f'Extracting article {i+1} of {len(infos)}')
    picture_file_name = os.path.basename(article['thumbnail']['url'])
    count_keyword = article['title'].count(keyword)+article['description'].count(keyword)
    contain_money = bool(re.search('(\$[\d,.]+)|([\d,.]+ (dollars|USD))', article['title']+article['description']))
    infos[i] = {
        'title': article['title'],
        'date': article['published_time'],
        'description': article['description'],
        'picture_file_name': picture_file_name,
        'count_keyword': count_keyword,
        'contain_money': contain_money
        }

    try:
        content = requests.get(article['thumbnail']['url']).content
        with open('output/'+picture_file_name, 'wb') as handler:
            handler.write(content)
    except Exception:
        logging.error('Download image error: '+traceback.format_exc().splitlines()[-1])
        infos[i]['picture_file_name'] = 'ERROR'

    logging.info(f'Completed article {i+1} of {len(infos)}')

def make_request(keyword:str, section:str, months:int):
    offset = 0
    query = {'offset': offset,
            'orderby': 'display_date:desc',
            'size': REQUEST_PAGE_SIZE,
            'website':'reuters'
            }
    end = datetime.now()
    start = end.replace(day=1)
    if(months > 1):
        start = start - dateutil.relativedelta.relativedelta(months=months-1)
    query['start_date'] = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    query['end_date'] = end.strftime('%Y-%m-%dT%H:%M:%SZ')
    query['keyword'] = keyword
    if(section):
        query['sections'] = '/'+section

    pages = []
    pages.append(request(query))
    total_size = pages[0]['result']['pagination']['total_size']
    while(offset + REQUEST_PAGE_SIZE < total_size):
        offset += REQUEST_PAGE_SIZE
        query['offset'] = offset
        pages.append(request(query))
    logging.info(f'Pages: {len(pages)}, Articles: {total_size}')
    return pages
        
def request(query):
    try:
        host_path = 'https://www.reuters.com/pf/api/v3/content/fetch/articles-by-search-v2?'
        query_plus = {'d':'204', '_website':'reuters'}
        url = host_path+parse.urlencode({'query':json.dumps(query)})+'&'+parse.urlencode(query_plus)
        logging.info(f'Request page URL: {url}')
        page = requests.get(url).json()
        if(page['statusCode'] >=200 and page['statusCode'] <=226):
            if('total_size' in page['result']['pagination']):
                return page
            else:
                raise StepException('REQUEST_EMPTY_PAGE', 'Empty page')
        else:
            raise StepException('REQUEST_RESPONSE_ERROR', 'StatusCode:'+page['statusCode'])
    except Exception:
        raise StepException('REQUEST_UNEXPECTED_ERROR', traceback.format_exc().splitlines()[-1])
    
def create_result_file(data):
    lib = Files()
    file_name = 'result_'+datetime.now().strftime('%Y%m%d%H%M%S')
    path = './output/'+file_name+'.xlsx'
    lib.create_workbook(path=path, fmt='xlsx')
    lib.create_worksheet(name='Result',content=data,header=True)
    lib.remove_worksheet('Sheet')
    lib.save_workbook()
    return path

class StepException(BaseException):
    def __init__(self, code, message) -> None:
        self.code = code
        self.message = message