import json
import pymongo as pm
import logging

# Logging configuration
logging.basicConfig(format='%(levelname)s : %(asctime)s %(message)s', level=logging.INFO)

# Mongo client configuration
mongo_client = pm.MongoClient('mongodb://app:rogus2015!@ds037601.mongolab.com:37601/goparty')
mongo_database = mongo_client.goparty
active_visits = mongo_database.activeVisits
finished_visits = mongo_database.finishedVisits

# Private helper methods
def __parse_event__(event_body):
    event_json = json.loads(event_body)

    return {'user_id': event_json.get('userId', None),
            'club_id': event_json.get('clubId', None),
            'timestamp': event_json.get('timestamp', None),
            'rating': event_json.get('rating', None),
            'qr_scan': event_json.get('payload', None)}


def __get_visit_key__(event_data):
    return {'user_id': event_data['user_id'], 'club_id': event_data['club_id']}


def __add_rating__(event_data):
    visit_id = __get_visit_key__(event_data)
    active_visits.find_one_and_update(visit_id, {'$set': {'rating': event_data['rating']}})


def __add_qr_scan__(event_data):
    visit_id = __get_visit_key__(event_data)
    active_visits.find_one_and_update(visit_id, {'$inc': {'qr_scanned': 1}})

def __start_visit__(event_data):
    visit_data = __get_visit_key__(event_data)
    visit_data['visit_start'] = event_data['timestamp']
    active_visits.insert_one(visit_data)


def __finish_visit__(event_data):
    visit_id = __get_visit_key__(event_data)
    finished_visit = active_visits.find_one_and_delete(visit_id)

    if finished_visit:
        finished_visit['visit_end'] = event_data['timestamp']
        finished_visit = __fill_empty_fields__(finished_visit)
        finished_visits.insert_one(finished_visit)
    else:
        logging.warning('FINISH VISIT - NOT ACTIVE VISIT - {}'.format(event_data))


def __fill_empty_fields__(event_data):
    if not event_data.get('qr_scanned', None):
        event_data['qr_scanned'] = 0

    if not event_data.get('rating', None):
        event_data['rating'] = 3

    return event_data


# Handler methods
def handle_checkin(ch, method, properties, body):
    '''
    Method handles incoming checkin event.
    After receiving a checkout event, the handler starts a new visit
    by creating an object in the activeVisits cache.
    '''
    event_data = __parse_event__(body)
    __start_visit__(event_data)
    logging.info('CHECKIN - {}'.format(body))


def handle_checkout(ch, method, properties, body):
    '''
    Method handles incoming checkout event.
    After receiving a checkout event, the handler looks for active visits
    in the activeVisits cache and moves them to the finishedVisits database.
    '''
    event_data = __parse_event__(body)
    __finish_visit__(event_data)
    logging.info('CHECKOUT - {}'.format(body))


def handle_rating(ch, method, properties, body):
    '''
    Method handles incoming rating event.
    After receiving a checkout event, the handler looks for active visits
    in the activeVisits cache and adds the rating of the current visit.
    '''
    event_data = __parse_event__(body)
    __add_rating__(event_data)
    logging.info('RATING - {}'.format(body))

def handle_qr_scan(ch, method, properties, body):
    '''
    Method handles incoming qr_scan_event.
    After receiving a qr_scan event, the handler looks for active visits
    in the activeVisits cache and increments the qr_scan counter.
    '''
    event_data = __parse_event__(body)
    __add_qr_scan__(event_data)
    logging.info('QRSCAN - {}'.format(body))