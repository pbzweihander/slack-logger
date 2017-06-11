import json
import requests


def es_create(index: str, doc_type: str, body: dict) -> bool:
    query = json.dumps(body)
    res = requests.post('http://localhost:9200/%s/%s/' % (index, doc_type), data=query)
    return json.loads(res.text).get('created') or False


def es_query_search(index: str, doc_type: str, query: dict) -> list:
    s_query = json.dumps(query)
    res = requests.post('http://localhost:9200/%s/%s/_search' % (index, doc_type), data=s_query)
    jsoned = json.loads(res.text)
    if jsoned.get('hits') and jsoned['hits'].get('hits'):
        return [(float(doc['sort'][0]), doc['_source']) for doc in jsoned['hits']['hits']]
    return []


#  'sort': [{'time': {'order': 'desc'}}],
def es_single_search(index: str, doc_type: str, body: dict, size=10, fr=None, sort=None) -> list:
    query = {
        'size': size,
        'query': {'term': body}}
    if sort:
        query['sort'] = sort
    if fr:
        query['search_after'] = [fr]
    return es_query_search(index, doc_type, query)


def es_filter_search(index: str, doc_type: str, filters: list, size=10, fr=None, sort=None) -> list:
    query = {
        'size': size,
        'sort': [{'time': {'order': 'desc'}}],
        'query': {'bool': {'filter': []}}}
    query['query']['bool']['filter'] = [{'term': {f[0]: f[1]}} for f in filters]
    if sort:
        query['sort'] = sort
    if fr:
        query['search_after'] = [fr]
    return es_query_search(index, doc_type, query)
