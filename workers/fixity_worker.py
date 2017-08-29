import hashlib
import requests
import pyqremis
import json
from uuid import uuid4
from datetime import datetime
from qremis_spiderlib.spider import QremisApiSpider
from qremis_spiderlib.lib import get_object_record, get_relationship_record, \
    get_event_record, seconds_since
from functools import partial
from redlock import RedLockFactory
import argparse
import logging


log = logging.getLogger(__name__)


def no_fixity_for(seconds, obj_id, qremis_api_url):
    log.debug("Retrieving obj rec from {} for id {}".format(qremis_api_url, obj_id))
    obj_rec = get_object_record(qremis_api_url, obj_id)
    log.debug("Object record received")
    rel_ids = []
    for x in obj_rec.get_linkingRelationshipIdentifier():
        if x.get_linkingRelationshipIdentifierType() == 'uuid':
            rel_ids.append(x.get_linkingRelationshipIdentifierValue())
    log.debug("Object has {} linked relationships".format(str(len(rel_ids))))
    event_rels = []
    for x in rel_ids:
        rel_rec = get_relationship_record(qremis_api_url, x)
        try:
            rel_rec.get_linkingEventIdentifier()
        except:
            continue
        event_rels.append(rel_rec)
    log.debug("{} relationships link to events".format(str(len(event_rels))))
    event_ids = []
    for x in event_rels:
        for y in x.get_linkingEventIdentifier():
            if y.get_linkingEventIdentifierType() == "uuid":
                event_ids.append(y.get_linkingEventIdentifierValue())
    fixity_event_recs = []
    for x in event_ids:
        event_rec = get_event_record(qremis_api_url, x)
        if event_rec.get_eventType() == "fixity check":
            fixity_event_recs.append(event_rec)
    log.debug("{} are fixity events".format(str(len(fixity_event_recs))))
    datetime_strs = []
    for x in fixity_event_recs:
        datetime_strs.append(x.get_eventDateTime())
    datetimes = []
    for x in datetime_strs:
        try:
            datetimes.append(datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
        except:
            # Invalid dt
            pass
    # The end of our filtering, if we got nothing return True
    if not datetimes:
        return True
    # Get the most recent date
    cur_max = datetimes[0]
    for x in datetimes:
        if cur_max < x:
            cur_max = x
    log.debug("Last fixity check at {}".format(str(cur_max)))
    if seconds_since(cur_max) > seconds:
        log.debug("Enough time has elapsed, returning True")
        return True
    else:
        log.debug("Enough time hasn't elapsed, returning False")
        return False


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
    parser.add_argument(
        "-v", "--verbosity", help="Logging verbosity",
        type=str,
        default="WARN"
    )
    parser.add_argument(
        "--fixity_max_age", help="How long ago a fixity event" +
        "can have occured before we run another - in seconds",
        type=int,
        default=60*60*24*7*4
    )
    args = parser.parse_args()

    logging.basicConfig(level=args.verbosity)
    logging.getLogger("urllib3").setLevel("WARN")

    fixity_check_cb = partial(fixity_check, args.archstor_api_url)
    filter_cb = partial(no_fixity_for, args.fixity_max_age)
    fac = RedLockFactory(connection_details=[{"host": x} for x in args.locking_server])
    spider = QremisApiSpider(
        args.qremis_api_url,
        filter_cb,
        fixity_check_cb,
        fac
    )
    spider.crawl(delay=args.delay)


if __name__ == "__main__":
    main()
