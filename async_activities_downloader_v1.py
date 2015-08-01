import requests
from concurrent.futures import ThreadPoolExecutor
import time


###################MARKETO############################
client_id = '***-***-***-***-***'
client_secret = '****'
endpoint_mrkt = 'https://****-***-***.mktorest.com'
#####################################################


def check_response(funk):
    def func_wrapper(*args, **kwargs):
        resp = funk(*args, **kwargs)
        try:
            if 'errors' in resp.keys():
                error_msg = resp['errors'][0]
                if error_msg['code'] == '604':
                    resp = funk(*args, **kwargs)
                elif error_msg['code'] == '601':
                    args[0].update_token()
                    resp = funk(*args, **kwargs)
                elif error_msg['code'] == '607':
                    print ("limit is done")
                else:
                    print (error_msg)
            return resp
        except Exception as Err:
            print("ERROR: in function " + funk.__name__ + " in response " + str(resp))
            print(type(Err), Err.args, Err)
            return resp
    return func_wrapper


class MarketoHandler:
    def __init__(self, client_id, client_secret, endpoint_mrkt):
        self.client_id = client_id
        self.client_secret = client_secret
        self.endpoint = endpoint_mrkt
        self.token = None
        self.update_token()


    def __repr__(self):
        return "<MarketoHandler('%s')>" % (self.token)

    @check_response
    def make_get_request(self, query_path, params, use_access_token=True):
        if use_access_token:
            params['access_token'] = self.token
        path = self.endpoint + query_path
        r = requests.get(url=path, params=params)
        return r.json()

    def get_paging_token(self, sincedate):
        params = {"sinceDatetime": sincedate}
        return self.make_get_request('/rest/v1/activities/pagingtoken.json', params)

    def update_token(self):
        params = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret}
        path = "/identity/oauth/token"
        resp = self.make_get_request(path, params, use_access_token=False)
        self.token = resp['access_token']

    def get_daily_stats(self):
        return self.make_get_request('/rest/v1/stats/usage.json', dict())


def make_request(rq):
    global total_reqs
    if total_reqs < query_limit:
        if not rq['npt']:
            date = MySQL_handler.get_last_date(rq['id'], rq['activitytype'])
            rq['npt'] = (M_handler.get_paging_token(date))['nextPageToken']  # nextpaging token
        params = prepare_params(rq['npt'], rq["activitytype"], rq['id'])
        total_reqs += 1
        print (total_reqs)
        r = requests.get(url=endpoint_mrkt + '/rest/v1/activities.json', params=params)
        return r
    else:
        return False


def prepare_params(nextPagingToken, ati, li):
    params = {
        "nextPageToken": nextPagingToken,
        "activityTypeIds": str(ati),
        "listId": str(li),
        "batchSize": "300",
        "access_token": M_handler.token}
    return params


def call_back_funk(j):
    result = j.result()
    if result:
        if result.status_code == 200:
            body = result.json()
            if body[u'success']:
                print (body)
                if body[u'moreResult']:
                    req = {'activitytype': activityType, 'npt': body[u'nextPageToken'], 'id': listId}
                    j = executor.submit(make_request, req)
                    j.add_done_callback(call_back_funk)
                else:
                    print("list " + listId + "activity " + str(activityType) + " is finished")
                    with open('/home/ayrat/Desktop/share/Email_Performance/Email Performance/list_done.txt', "a") as h:
                        h.write(str(listId)+'-'+str(activityType)+'\n')
            else:
                if body[u'errors'][0][u'code'] == u'601':  # means u'Access token invalid'
                    M_handler.update_token()
                    j = executor.submit(resend_request)
                    j.add_done_callback(call_back_funk)  # repeating request
                elif body[u'errors'][0][u'code'] == u'607':
                    pass
                elif body[u'errors'][0][u'code'] == u'604':  # Request timed out
                    j = executor.submit(resend_request)
                    j.add_done_callback(call_back_funk)  # repeating request
                else:
                    print(body[u'errors'])
        else:
            print (result.status_code, result.text)




M_handler = MarketoHandler(client_id, client_secret, endpoint_mrkt)
query_limit = 10000

all_reqs = []  ####
temp = {'excl': cond['excl'], 'incl': cond['incl'], 'npt': None, 'id':cond['id'], 'name':cond['name'], 'activitytype':at}
all_reqs.append(temp)


total_reqs = 0

executor = ThreadPoolExecutor(max_workers=30)


for i in all_reqs:
    time.sleep(5) #otherwise pool gets overloaded and executor can not handle tasks
    job = executor.submit(make_request, i)     ### start the thread
    job.add_done_callback(call_back_funk)


### TODO merge request methods in c.f. and Marketo handler


