import pyqremis
import requests


def response_200_json(r):
    if not r.status_code == 200:
        raise ValueError()
    try:
        rj = r.json()
    except:
        raise
    return rj


def _get_entity(kls, qremis_api_url, list_url_fragment, id):
    url = qremis_api_url + list_url_fragment + id
    r = requests.get(url)
    rj = response_200_json(r)
    return kls.from_dict(rj)


def get_object(archstor_url, identifier, of):
    r = requests.get(archstor_url + identifier, stream=True)
    for chunk in r.iter_content(chunk_size=1024):
        if chunk:
            of.write(chunk)


def get_object_record(qremis_api_url, objId):
    return _get_entity(pyqremis.Object, qremis_api_url, "/object_list/", objId)


def get_event_record(qremis_api_url, eventId):
    return _get_entity(pyqremis.Event, qremis_api_url, "/event_list/", eventId)


def get_relationship_record(qremis_api_url, relId):
    return _get_entity(pyqremis.Relationship, qremis_api_url, "/relationship_list/", relId)


def get_agent_record(qremis_api_url, agentId):
    return _get_entity(pyqremis.Agent, qremis_api_url, "/agent_list/", agentId)


def get_rights_record(qremis_api_url, rightsId):
    return _get_entity(pyqremis.Rights, qremis_api_url, "/rights_list/", rightsId)
