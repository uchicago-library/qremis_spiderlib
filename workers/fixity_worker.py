import hashlib
import requests
import pyqremis
import json
from uuid import uuid4
from datetime import datetime
from qremis_spiderlib.spider import QremisApiSpider
from qremis_spiderlib.lib import get_object_record
from functools import partial
from qremis_spiderlib.filter_callbacks import no_filter
from redlock import RedLockFactory
import argparse


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
        qremis_api_url + "/relationship_list", data={"record": json.dumps(relationship.to_dict())}
    )
    if relationship_post_response.status_code != 200:
        raise ValueError()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--qremis_api_url", help="The URL of the qremis API",
        required=True,
        type=str
    )
    parser.add_argument(
        "--archstor_api_url", help="The URL of the archstor API",
        required=True,
        type=str
    )
    # TODO: Figure out how to parse all the address details
    # out of these args
    parser.add_argument(
        "--locking_server", help="The addresses of the redis " +
        "locking servers",
        required=True,
        type=str,
        action='append'
    )
    parser.add_argument(
        "--delay", help="The delay between actions",
        type=float,
        default=.1
    )
    args = parser.parse_args()


    fixity_check_cb = partial(fixity_check, args.archstor_api_url)
    fac = RedLockFactory(connection_details=[{"host": x} for x in args.locking_server])
    spider = QremisApiSpider(
        args.qremis_api_url,
        no_filter,
        fixity_check_cb,
        fac
    )
    spider.crawl(delay=args.delay)


if __name__ == "__main__":
    main()
