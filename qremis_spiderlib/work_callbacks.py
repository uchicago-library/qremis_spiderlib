import tempfile
import hashlib
from .lib import get_object_record
import requests
import pyqremis
import json
from uuid import uuid4
from datetime import datetime


def fixity_check(archstor_api_url, identifier, qremis_api_url):
    rec = get_object_record(qremis_api_url, identifier)
    rec_md5 = None
    for x in rec.get_objectCharacteristics():
        for y in x.get_fixity():
            if y.get_messageDigestAlgorithm() == 'md5':
                if rec_md5 is not None:
                    raise ValueError()
                rec_md5 = y.get_messageDigest()
    if rec_md5 is None:
        raise ValueError()
    hasher = hashlib.md5()
    r = requests.get(archstor_api_url+identifier, stream=True)
    for chunk in r.iter_content(chunk_size=8096):
        if chunk:
            hasher.update(chunk)

    event = pyqremis.Event(
        eventIdentifier=pyqremis.EventIdentifier(
            eventIdentifierType="uuid",
            eventIdentifierValue=uuid4().hex
        ),
        eventDateTime=str(datetime.now()),
        eventType="fixity check",
        eventDetailInformation=pyqremis.EventDetailInformation(
            eventDetail="fixity checked via md5 by the fixity checker service"
        ),
        eventOutcomeInformation=pyqremis.EventOutcomeInformation(
            eventOutcome="SUCCESS" if hasher.hexdigest() == rec_md5 else "FAIL",
            eventOutcomeDetail=pyqremis.EventOutcomeDetail(
                eventOutcomeDetailNote="Computed md5: {}".format(hasher.hexdigest())
            )
        )
    )

    relationship = pyqremis.Relationship(
        relationshipIdentifier=pyqremis.RelationshipIdentifier(
            relationshipIdentifierType="uuid",
            relationshipIdentifierValue=uuid4().hex
        ),
        relationshipType="link",
        relationshipSubType="simple",
        relationshipNote="links the object to a fixity check",
        linkingObjectIdentifier=pyqremis.LinkingObjectIdentifier(
            linkingObjectIdentifierType="uuid",
            linkingObjectIdentifierValue=identifier
        ),
        linkingEventIdentifier=pyqremis.LinkingEventIdentifier(
            linkingEventIdentifierType="uuid",
            linkingEventIdentifierValue=event.get_eventIdentifier()[0].get_eventIdentifierValue()
        )
    )

    event_post_response = requests.post(
        qremis_api_url + "/event_list", data={"record": json.dumps(event.to_dict())}
    )
    if event_post_response.status_code != 200:
        raise ValueError()
    relationship_post_response = requests.post(
        qremis_api_url +"/relationship_list", data={"record": json.dumps(relationship.to_dict())}
    )
    if relationship_post_response.status_code != 200:
        raise ValueError()
