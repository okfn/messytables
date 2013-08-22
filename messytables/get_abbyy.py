"""curl's commands were
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
try:
    from abbyy_secret import user
except:
    raise RuntimeError("""abbyy_secret.py needs to contain
user=(abbyy_application, abbyy_secret_key)""")
baseurl = "http://cloud.ocrsdk.com/%s"


class ABBYYError(Exception):
    pass


def handle_response(response):
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
    assert len(tasks) == 1
    return tasks[0].attrib


def process_image(file_name):
    params = {'exportFormat': 'xml',
              'language': 'English'
              }

    url = baseurl % "processImage"
    #url = 'http://httpbin.org/post'

    with open(file_name, 'rb') as upload_file:
        payload = {'upload': upload_file}
        resp = requests.post(url, auth=user, files=payload, params=params)
    return handle_response(resp)


def get_task_status(task):
    url = baseurl % "getTaskStatus"
    resp = requests.get(url, auth=user, data={'taskId': task})
    return handle_response(resp)


def get_ocr_url(filename):
    logging.info("start process")
    status = process_image(filename)
    while "resultUrl" not in status:
        wait = int(status.get("estimatedProcessingTime", "0"))
        logging.info("sleeping for %r seconds" % wait)
        time.sleep(wait)
        logging.info("get task status")
        status = get_task_status(status['id'])
    return status['resultUrl']


def get_ocr_content(filename):
    url = get_ocr_url(filename)
    return requests.get(url).content

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print get_ocr_content("../horror/t1.TIF")[:4000]
