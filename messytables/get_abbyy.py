"""curl's commands wereV
-s # silent -- ignore
-S # show error -- ignore
--user "fake_APPID:fake_PWD"
--form "upload=@input"
"http://cloud.ocrsdk.com/processImage?exportFormat=output_f&language=lang"
"""

import requests
import lxml.etree
import time
import logging
import hashlib
import os
from os.path import join, isfile

try:
    import abbyy_secret
except ImportError:
    pass

baseurl = "http://cloud.ocrsdk.com/%s"

IMAGE_PARAMS = {'exportFormat': 'xml',
                'language': 'English',
                'correctOrientation': 'false',
                'correctSkew': 'false'
                }

CACHE_DIR = '/tmp'


class ABBYYError(Exception):
    pass

class ABBYYAuthError(Exception):
    pass

def user():
    try:
        return abbyy_secret.user
    except:
        raise ABBYYAuthError("""abbyy_secret.py needs to contain
user=(abbyy_application, abbyy_secret_key)""")



def handle_response(response, as_list=False):
    try:
        root = lxml.etree.fromstring(response.content)
    except lxml.etree.XMLSyntaxError:
        raise ABBYYError("%r: NO CONTENT" % (response.status_code))
    errormsg = root.xpath("//error/message/text()")
    if errormsg:
        raise ABBYYError("%r: %r" % (response.status_code, errormsg))
    errormsg = root.xpath("//response/task/@error")
    if errormsg:
        raise ABBYYError("%r: %r" % (response.status_code, errormsg))
    tasks = root.xpath("//response/task")
    if as_list:
        return [x.attrib for x in tasks]
    else:
        assert len(tasks) == 1
        return tasks[0].attrib


def list_tasks():
    return handle_response(requests.get(baseurl % 'listTasks', auth=user()),
                           as_list=True)


def is_task(descr):
    matching = [task for task in list_tasks()
                if task.get('description', '***') == descr]
    if not matching:
        return None
    if len(matching) > 1:
        logging.warn("More than one ABBYY description match for %r" % descr)
    return matching[0]


def get_task_status(task):
    url = baseurl % "getTaskStatus"
    resp = requests.get(url, auth=user(), data={'taskId': task})
    return handle_response(resp)


def wait_for_response(status=None):
    if not status:
        status = {}
    while "resultUrl" not in status:
        logging.info(status)
        wait = int(status.get("estimatedProcessingTime", "0"))
        logging.info("sleeping for %r seconds" % wait)
        time.sleep(wait)
        logging.info("get task status")
        status = get_task_status(status['id'])
    logging.info(status)
    return status['resultUrl']


class OCRFile(object):
    def __init__(self, fh):
        self.fh = fh
        self.md5 = self.md5hash()
        user()  # fail early

    def md5hash(self):
        try:
            self.fh.seek(0)
        except:  # TODO appropriate error!
            return None
        _hash = hashlib.md5(self.fh.read())
        _hash.update(repr(IMAGE_PARAMS))
        hd = _hash.hexdigest()
        self.fh.seek(0)
        return hd

    def process_image(self):
        url = baseurl % "processImage"
        payload = {'upload': self.fh}
        params = dict(IMAGE_PARAMS)
        params['description'] = self.md5
        resp = requests.post(url, auth=user(), files=payload, params=params)
        return handle_response(resp)

    def get_ocr_url(self):
        logging.info("start process")
        previous_result = is_task(self.md5)
        if previous_result:
            logging.info("cache hit")
            if 'resultUrl' in previous_result:  # handle race condition, more testing definately required
                return previous_result['resultUrl']
            else:
                return wait_for_response(previous_result)
        status = self.process_image()
        return wait_for_response(status)

    def get_ocr_content(self, cache=True):
        cache_fn = join(CACHE_DIR, 'abbyy_{}.xml'.format(self.md5))
        if cache and isfile(cache_fn):
            with open(cache_fn, 'rb') as f:
                return f.read()

        response = requests.get(self.get_ocr_url())
        response.raise_for_status()
        with open(cache_fn, 'wb') as f:
            f.write(response.content)

        return response.content

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    list_tasks()
    with open(sys.argv[1], "rb") as fh:
        ocr = OCRFile(fh)
        content = ocr.get_ocr_content(fh)
        with open("output", "wb") as outfile:
            outfile.write(content)
        print "see output"
