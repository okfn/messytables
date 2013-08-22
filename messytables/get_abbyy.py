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
try:
    from abbyy_secret import user
except:
    raise RuntimeError("""abbyy_secret.py needs to contain
user=(abbyy_application, abbyy_secret_key)""")

baseurl = "http://cloud.ocrsdk.com/%s"


class ABBYYError(Exception):
    pass


def md5hash(fh):
    try:
        fh.seek(0)
    except:  # TODO appropriate error!
        return None
    _hash = hashlib.md5(fh.read()).hexdigest()
    fh.seek(0)
    return _hash


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
    return handle_response(requests.get(baseurl % 'listTasks', auth=user),
                           as_list=True)


def is_task(descr):
    matching = [task for task in list_tasks()
                if task.get('description', '***') == descr]
    if not matching:
        return None
    if len(matching) > 1:
        logging.warn("More than one ABBYY description match for %r" % descr)
    return matching[0]


def process_image(filehandle):
    params = {'exportFormat': 'xml',
              'language': 'English',
              'description': md5hash(filehandle) or ''
              }

    url = baseurl % "processImage"
    #url = 'http://httpbin.org/post'

    payload = {'upload': filehandle}
    resp = requests.post(url, auth=user, files=payload, params=params)
    return handle_response(resp)


def get_task_status(task):
    url = baseurl % "getTaskStatus"
    resp = requests.get(url, auth=user, data={'taskId': task})
    return handle_response(resp)


def get_ocr_url(filehandle):
    logging.info("start process")
    previous_result = is_task(md5hash(filehandle))
    if previous_result:
        logging.info("cache hit")
        return previous_result['resultUrl']
    status = process_image(filehandle)
    while "resultUrl" not in status:
        logging.info(status)
        wait = int(status.get("estimatedProcessingTime", "0"))
        logging.info("sleeping for %r seconds" % wait)
        time.sleep(wait)
        logging.info("get task status")
        status = get_task_status(status['id'])
    logging.info(status)
    return status['resultUrl']


def get_ocr_content(filehandle, cache=True):
    #if cache:
    #    cachedata = get_cache(filehandle)
    #    if cachedata:
    #        return cachedata
    url = get_ocr_url(filehandle)
    ocr_data = requests.get(url).content
    #if cache:
    #    save_cache(filehandle, ocr_data)
    return ocr_data

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    list_tasks()
    with open("../horror/t1.TIF", "rb") as fh:
        print get_ocr_content(fh)[:4000]
