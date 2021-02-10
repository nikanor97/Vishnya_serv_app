import pymongo

from flask import Flask, jsonify, abort, request
from pymongo import MongoClient
from datetime import datetime

BASE_URL = '/vishnya/1.0'
# ATLAS_URL = 'mongodb+srv://admin:admin@cluster0.llpwn.mongodb.net/serv_app?retryWrites=true&w=majority'

# l = 'admin.root'
# p = '6pjuPwubHbRwrKeZU4szAPev8h8FJGxZR7P7'
#
# MCS_URL = 'mongodb://admin:adminadminadminadmin@185.86.144.250'

client = MongoClient()
# client.admin.authenticate(l, p)

db = client.serv_app
salon_coll = db.salon
master_coll = db.master
client_coll = db.client_
# print(salon_coll)
# post_id = salon_coll.insert_one({'test': 'yes'}).inserted_id
# print(post_id)
app = Flask(__name__)


@app.route(f'{BASE_URL}/salon/reg', methods=['POST'])
def salon_reg():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        name = data.get('name')
        phone = data.get('phone')
        # address = data.get('address')

        if name is None or phone is None:
            error_msg = 'Please enter required parameters'
            # error = True
            raise ValueError

        search_results = salon_coll.find({'name': name, 'phone': phone})
        search_docs = [i for i in search_results]
        if len(search_docs) > 0:
            error_msg = f'Salon with name "{name}" and phone "{phone}" is already registered'
            # error = True
            raise ValueError

        post = data
        post['masters'] = []
        salon_coll.insert_one(post)
        return 'Successfully added a new salon to db'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/salon/del', methods=['POST'])
def salon_del():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        # name = data.get('name')
        phone = data.get('phone')

        if phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_salons = salon_coll.find({'phone': phone})
        found_salons = [i for i in search_salons]
        if len(found_salons) == 0:
            error_msg = f'Salon with phone "{phone}" does not exist in db'
            raise ValueError

        masters = found_salons[0].get('masters')
        if masters is None:
            masters = []
        if len(masters) != 0:
            error_msg = f'There are masters working in the salon, so it can not be deleted'
            raise ValueError

        salon_id = found_salons[0]['_id']
        print(salon_id)
        salon_coll.delete_one({'_id': salon_id})

        return 'Successfully removed a salon from db'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/salon/get_masters', methods=['POST'])
def salon_get_masters():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        salon_phone = data.get('phone')

        if salon_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_salons = salon_coll.find({'phone': salon_phone})
        found_salons = [i for i in search_salons]
        if len(found_salons) == 0:
            error_msg = f'Salon with phone "{salon_phone}" does not exist in db'
            raise ValueError

        masters = found_salons[0].get('masters')
        if masters is None:
            masters = []
        return {'result': masters}
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/salon/get_profile', methods=['POST'])
def salon_get_profile():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        salon_phone = data.get('phone')

        if salon_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_salons = salon_coll.find({'phone': salon_phone})
        found_salons = [i for i in search_salons]
        if len(found_salons) == 0:
            error_msg = f'Salon with phone "{salon_phone}" does not exist in db'
            raise ValueError

        del found_salons[0]['_id']
        return {'result': found_salons[0]}
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/reg', methods=['POST'])
def master_reg():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        # name = data.get('name')
        phone = data.get('phone')
        master_type = data.get('master_type')
        experience = data.get('experience')
        salon_phone = data.get('salon')

        if phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError
        if master_type is not None:
            if master_type not in [0, 1, 2]:
                error_msg = 'Variable "master_type" should be equal to 0, 1 or 2'
                raise ValueError
        if experience is not None:
            if experience < 0:
                error_msg = 'Experience should be written in years and be greater or equal to 0'
                raise ValueError

        search_results = master_coll.find({'phone': phone})
        search_docs = [i for i in search_results]
        if len(search_docs) > 0:
            error_msg = f'Master with phone "{phone}" is already registered'
            raise ValueError

        if salon_phone is not None:
            search_salons = salon_coll.find({'phone': salon_phone})
            found_salons = [i for i in search_salons]
            print(found_salons)
            if len(found_salons) == 0:
                error_msg = f'Salon with phone "{salon_phone}" does not exist in db'
                raise ValueError
            salon_masters = found_salons[0]['masters']
            if salon_masters is None:
                salon_masters = []
            salon_masters.append(phone)
            salon_id = found_salons[0]['_id']
            salon_coll.update({'_id': salon_id},
                              {'$set': {
                                  'masters': salon_masters
                              }}, upsert=False)

        post = data
        master_coll.insert_one(post)
        return 'Successfully added a new master to db'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/change_salon', methods=['POST'])
def master_change_salon():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        master_phone = data.get('master_phone')
        salon_phone = data.get('salon_phone')

        if master_phone is None or salon_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_masters = master_coll.find({'phone': master_phone})
        found_masters = [i for i in search_masters]
        if len(found_masters) == 0:
            error_msg = f'Master with phone "{master_phone}" can not ne found in db'
            raise ValueError

        search_salons = salon_coll.find({'phone': salon_phone})
        found_salons = [i for i in search_salons]
        if len(found_salons) == 0:
            error_msg = f'Salon with phone "{salon_phone}" does not exist in db'
            raise ValueError

        current_master_salon = found_masters[0].get('salon')
        if current_master_salon is not None:

            if current_master_salon == salon_phone:
                error_msg = f'Master with phone "{master_phone}" is already attached to salon with phone "{salon_phone}"'
                raise ValueError

            search_salons_current = salon_coll.find({'phone': current_master_salon})
            found_salons_current = [i for i in search_salons_current]
            current_salon_masters = found_salons_current[0]['masters']
            current_salon_masters.remove(master_phone)

            current_salon_id = found_salons_current[0]['_id']
            salon_coll.update({'_id': current_salon_id},
                               {'$set': {
                                   'masters': current_salon_masters
                               }}, upsert=False)

        new_salon_masters = found_salons[0].get('masters')
        if new_salon_masters is None:
            new_salon_masters = []
        new_salon_masters.append(master_phone)

        new_salon_id = found_salons[0]['_id']
        salon_coll.update({'_id': new_salon_id},
                           {'$set': {
                               'masters': new_salon_masters
                           }}, upsert=False)

        master_id = found_masters[0]['_id']
        master_coll.update({'_id': master_id},
                           {'$set': {
                               'salon': salon_phone
                           }}, upsert=False)

        return f'Successfully changed salon for master with phone "{master_phone}"'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/del', methods=['POST'])
def master_del():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        phone = data.get('phone')

        if phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_masters = master_coll.find({'phone': phone})
        found_masters = [i for i in search_masters]
        if len(found_masters) == 0:
            error_msg = f'Master with phone "{phone}" not found in db'
            raise ValueError

        master_appointments = found_masters[0].get('appointments')
        if master_appointments is None:
            master_appointments = []
        current_datetime = datetime.now()
        for appointment in master_appointments:
            if appointment['start'] > current_datetime:
                search_clients = client_coll.find({'phone': appointment['client']})
                found_clients = [i for i in search_clients]
                client_id = found_clients[0]['_id']
                print(found_clients[0]['phone'])
                client_appointments = found_clients[0]['appointments']
                for client_appointment in client_appointments:
                    if client_appointment['start'] == appointment['start'] and \
                            client_appointment['end'] == appointment['end'] and \
                            client_appointment['master'] == phone:
                        client_appointments.remove(client_appointment)
                        break
                client_coll.update({'_id': client_id},
                                   {'$set': {
                                       'appointments': client_appointments
                                   }}, upsert=False)

        master_salon = found_masters[0].get('salon')
        if master_salon is not None:
            search_salons = salon_coll.find({'phone': master_salon})
            found_salons = [i for i in search_salons]
            salon_masters = found_salons[0]['masters']
            salon_masters.remove(phone)

            salon_id = found_salons[0]['_id']
            salon_coll.update({'_id': salon_id},
                               {'$set': {
                                   'masters': salon_masters
                               }}, upsert=False)

        master_id = found_masters[0]['_id']
        print(master_id)
        master_coll.delete_one({'_id': master_id})
        # master_coll.delete_one(master_id)

        return 'Successfully removed master from db'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/get_profile', methods=['POST'])
def master_get_profile():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        master_phone = data.get('phone')

        if master_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_masters = master_coll.find({'phone': master_phone})
        found_masters = [i for i in search_masters]
        if len(found_masters) == 0:
            error_msg = f'Master with phone "{master_phone}" does not exist in db'
            raise ValueError

        del found_masters[0]['_id']
        return {'result': found_masters[0]}
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


def dt_to_datetime(dt: str):
    datetime_dt = datetime.strptime(dt[:-8], "%Y-%m-%dT%H:%M")
    return datetime_dt


@app.route(f'{BASE_URL}/master/add_working_hours', methods=['POST'])
def master_add_working_hours():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        working_hours = data.get('working_hours')
        phone = data.get('phone')

        if working_hours is None or phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        dt_start = working_hours.get('dt_start')
        dt_end = working_hours.get('dt_end')

        if dt_start is None or dt_end is None:
            error_msg = 'Please enter working hours interval as required'
            raise ValueError

        datetime_start = dt_to_datetime(dt_start)
        datetime_end = dt_to_datetime(dt_end)

        if datetime_start >= datetime_end:
            error_msg = 'Start of time interval should be earlier than it`s end'
            raise ValueError

        search_masters = master_coll.find({'phone': phone})
        found_masters = [i for i in search_masters]

        if len(found_masters) == 0:
            error_msg = f'Master with phone"{phone}" has not been found in db'
            raise ValueError

        old_working_hours = found_masters[0].get('working_hours')
        if old_working_hours is None:
            old_working_hours = []

        #         [        ]   [               ]
        #                {         }
        # new_working_hour_valid = True
        # conflicting_working_hours = None
        # for idx, wh in enumerate(old_working_hours):
        #     if wh['start'] <= datetime_start <= datetime_end <= wh['end']:
        #         new_working_hour_valid = False
        #         conflicting_working_hours = wh
        #         break
        #     if wh['start'] <= datetime_start <= wh['end'] < datetime_end:
        #         old_working_hours[idx]['end'] = datetime_end
        #     # нужен if, а не elif, т.к. закрывает больше условий
        #     if datetime_start < wh['start'] <= datetime_end <= wh['end']:
        #         old_working_hours[idx]['start'] = datetime_start
        #
        # if not new_working_hour_valid:
        #     error_msg = 'New working hour'

        old_working_hours.append({'start': datetime_start,
                                  'end': datetime_end})
        master_id = found_masters[0]['_id']
        master_coll.update({'_id': master_id},
                           {'$set': {
                               'working_hours': old_working_hours
                           }}, upsert=False)

        return f'Successfully added a new working hours for master with phone "{phone}"'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/remove_working_hours', methods=['POST'])
def master_remove_working_hours():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        working_hours = data.get('working_hours')
        phone = data.get('phone')

        if working_hours is None or phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        dt_start = working_hours.get('dt_start')
        dt_end = working_hours.get('dt_end')

        if dt_start is None or dt_end is None:
            error_msg = 'Please enter working hours interval as required'
            raise ValueError

        datetime_start = dt_to_datetime(dt_start)
        datetime_end = dt_to_datetime(dt_end)

        if datetime_start >= datetime_end:
            error_msg = 'Start of time interval should be earlier than it`s end'
            raise ValueError

        search_masters = master_coll.find({'phone': phone})
        found_masters = [i for i in search_masters]

        if len(found_masters) == 0:
            error_msg = f'Master with phone"{phone}" has not been found in db'
            raise ValueError

        master_appointments = found_masters[0].get('appointments')
        if master_appointments is None:
            master_appointments = []
        for appointment in master_appointments:
            if datetime_start <= appointment['start'] < datetime_end or \
                    datetime_start < appointment['end'] <= datetime_end or \
                    datetime_start >= appointment['start'] and appointment['end'] >= datetime_end:
                error_msg = f'Entered time interval intersects with appointments for master with phone "{phone}"'
                raise ValueError

        old_working_hours = found_masters[0].get('working_hours')
        if old_working_hours is None:
            old_working_hours = []
        idxs_to_remove = []
        for idx, wh in enumerate(old_working_hours):
            if datetime_start < wh['start']:
                if datetime_end <= wh['start']:
                    pass
                elif datetime_end < wh['end']:
                    old_working_hours[idx]['start'] = datetime_end
                elif datetime_end >= wh['end']:
                    idxs_to_remove.append(idx)
            elif datetime_start == wh['start']:
                if datetime_end < wh['end']:
                    old_working_hours[idx]['start'] = datetime_end
                elif datetime_end >= wh['end']:
                    idxs_to_remove.append(idx)
            elif wh['start'] < datetime_start < wh['end']:
                if datetime_end < wh['end']:
                    old_working_hours.append({'start': datetime_end, 'end': wh['end']})
                    old_working_hours[idx]['end'] = datetime_start
                elif datetime_end >= wh['end']:
                    old_working_hours[idx]['end'] = datetime_start
            elif datetime_start >= wh['end']:
                pass

        for idx in reversed(idxs_to_remove):
            del old_working_hours[idx]

        master_id = found_masters[0]['_id']
        master_coll.update({'_id': master_id},
                           {'$set': {
                               'working_hours': old_working_hours
                           }}, upsert=False)

        return f'Successfully removed working hours for master with phone "{phone}"'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/get_working_hours', methods=['POST'])
def master_get_working_hours():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        time_interval = data.get('time_interval')
        master_phone = data.get('master_phone')

        if time_interval is None or master_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        dt_start = time_interval.get('dt_start')
        dt_end = time_interval.get('dt_end')

        if dt_start is None or dt_end is None:
            error_msg = 'Please enter time interval as required'
            raise ValueError

        datetime_start = dt_to_datetime(dt_start)
        datetime_end = dt_to_datetime(dt_end)

        if datetime_start >= datetime_end:
            error_msg = 'Start of time interval should be earlier than it`s end'
            raise ValueError

        search_masters = master_coll.find({'phone': master_phone})
        found_masters = [i for i in search_masters]

        if len(found_masters) == 0:
            error_msg = f'Master with phone"{master_phone}" has not been found in db'
            raise ValueError

        working_hours = found_masters[0].get('working_hours')
        if working_hours is None:
            working_hours = []
        result_wh = []
        for wh in working_hours:
            if datetime_start <= wh['start'] < wh['end'] <= datetime_end:
                result_wh.append(wh)
            elif wh['start'] < datetime_start < wh['end'] <= datetime_end:
                result_wh.append({'start': datetime_start, 'end': wh['end']})
            elif datetime_start <= wh['start'] < datetime_end < wh['end']:
                result_wh.append({'start': wh['start'], 'end': datetime_end})

        result = [{'dt_start': i['start'],
                   'dt_end': i['end']
                   } for i in result_wh]

        return {'result': result}
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/add_appointment', methods=['POST'])
def master_add_appointment():
    error_msg = 'Error occurred'
    try:
        data = request.get_json()
        print(data)
        master_phone = data.get('master_phone')
        client_phone = data.get('client_phone')
        time_interval = data.get('time_interval')

        if time_interval is None or master_phone is None or client_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        dt_start = time_interval.get('dt_start')
        dt_end = time_interval.get('dt_end')

        if dt_start is None or dt_end is None:
            error_msg = 'Please enter working hours interval as required'
            raise ValueError

        datetime_start = dt_to_datetime(dt_start)
        datetime_end = dt_to_datetime(dt_end)

        if datetime_start >= datetime_end:
            error_msg = 'Start of time interval should be earlier than it`s end'
            raise ValueError

        search_masters = master_coll.find({'phone': master_phone})
        found_masters = [i for i in search_masters]

        if len(found_masters) == 0:
            error_msg = f'Master with phone"{master_phone}" has not been found in db'
            raise ValueError

        search_clients = client_coll.find({'phone': client_phone})
        found_clients = [i for i in search_clients]

        if len(found_clients) == 0:
            error_msg = f'Client with phone"{client_phone}" has not been found in db'
            raise ValueError

        working_hours = found_masters[0].get('working_hours')
        if working_hours is None:
            working_hours = []
        appointment_dt_valid_for_master_working_hours = False
        for wh in working_hours:
            if wh['start'] <= datetime_start and wh['end'] >= datetime_end:
                appointment_dt_valid_for_master_working_hours = True
                break

        if not appointment_dt_valid_for_master_working_hours:
            error_msg = f'Master with phone "{master_phone}" has not working hours in complete interval from "{datetime_start}" to "{datetime_end}"'
            raise ValueError

        master_appointments = found_masters[0].get('appointments')
        if master_appointments is None:
            master_appointments = []
        appointment_dt_valid_for_master_appointments = True
        conflict_appointment_start = None
        conflict_appointment_end = None
        for ma in master_appointments:
            if datetime_start <= ma['start'] < datetime_end \
                    or datetime_start < ma['end'] <= datetime_end \
                    or datetime_start <= ma['start'] and datetime_end >= ma['end'] \
                    or datetime_start >= ma['start'] and datetime_end <= ma['end']:
                appointment_dt_valid_for_master_appointments = False
                conflict_appointment_start = ma['start']
                conflict_appointment_end = ma['end']
                break

        if not appointment_dt_valid_for_master_appointments:
            error_msg = f'Master with phone "{master_phone}" has intersection with another appointment with time interval from "{conflict_appointment_start}" to "{conflict_appointment_end}"'
            raise ValueError

        client_appointments = found_clients[0].get('appointments')
        if client_appointments is None:
            client_appointments = []
        appointment_dt_valid_for_client = True
        conflict_appointment_start = None
        conflict_appointment_end = None
        for ca in client_appointments:
            if datetime_start <= ca['start'] < datetime_end \
                    or datetime_start < ca['end'] <= datetime_end \
                    or datetime_start <= ca['start'] and datetime_end >= ca['end'] \
                    or datetime_start >= ca['start'] and datetime_end <= ca['end']:
                appointment_dt_valid_for_client = False
                conflict_appointment_start = ca['start']
                conflict_appointment_end = ca['end']
                break

        if not appointment_dt_valid_for_client:
            error_msg = f'Client with phone "{client_phone}" has intersection with another appointment with time interval from "{conflict_appointment_start}" to "{conflict_appointment_end}"'
            raise ValueError

        master_appointments = found_masters[0].get('appointments')
        if master_appointments is None:
            master_appointments = []

        master_appointments.append({'start': datetime_start,
                                    'end': datetime_end,
                                    'client': client_phone})
        master_id = found_masters[0]['_id']
        master_coll.update({'_id': master_id},
                           {'$set': {
                               'appointments': master_appointments
                           }}, upsert=False)

        client_appointments = found_clients[0].get('appointments')
        if client_appointments is None:
            client_appointments = []

        client_appointments.append({'start': datetime_start,
                                    'end': datetime_end,
                                    'master': master_phone})
        client_id = found_clients[0]['_id']
        client_coll.update({'_id': client_id},
                           {'$set': {
                               'appointments': client_appointments
                           }}, upsert=False)

        return f'Successfully added an appointment for master with phone "{master_phone}" and client with phone "{client_phone}"'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/get_appointments', methods=['POST'])
def master_get_appointments():
    error_msg = 'Error occurred'
    try:
        data = request.get_json()
        print(data)
        master_phone = data.get('master_phone')
        time_interval = data.get('time_interval')

        if time_interval is None or master_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        dt_start = time_interval.get('dt_start')
        dt_end = time_interval.get('dt_end')

        if dt_start is None or dt_end is None:
            error_msg = 'Please enter working hours interval as required'
            raise ValueError

        datetime_start = dt_to_datetime(dt_start)
        datetime_end = dt_to_datetime(dt_end)

        if datetime_start >= datetime_end:
            error_msg = 'Start of time interval should be earlier than it`s end'
            raise ValueError

        search_masters = master_coll.find({'phone': master_phone})
        found_masters = [i for i in search_masters]

        if len(found_masters) == 0:
            error_msg = f'Master with phone"{master_phone}" has not been found in db'
            raise ValueError

        master_appointments = found_masters[0].get('appointments')
        if master_appointments is None:
            master_appointments = []
        result_appointments = []
        for ma in master_appointments:
            if datetime_start <= ma['start'] and ma['end'] <= datetime_end:
                result_appointments.append(ma)

        result = [{'master_phone': master_phone,
                   'client_phone': i['client'],
                   'time_interval': {
                       'dt_start': i['start'],
                       'dt_end': i['end']
                   }} for i in result_appointments]

        return {'result': result}
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/client/get_appointments', methods=['POST'])
def client_get_appointments():
    error_msg = 'Error occurred'
    try:
        data = request.get_json()
        print(data)
        client_phone = data.get('client_phone')
        time_interval = data.get('time_interval')

        if time_interval is None or client_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        dt_start = time_interval.get('dt_start')
        dt_end = time_interval.get('dt_end')

        if dt_start is None or dt_end is None:
            error_msg = 'Please enter working hours interval as required'
            raise ValueError

        datetime_start = dt_to_datetime(dt_start)
        datetime_end = dt_to_datetime(dt_end)

        if datetime_start >= datetime_end:
            error_msg = 'Start of time interval should be earlier than it`s end'
            raise ValueError

        search_clients = client_coll.find({'phone': client_phone})
        found_clients = [i for i in search_clients]

        if len(found_clients) == 0:
            error_msg = f'Client with phone"{client_phone}" has not been found in db'
            raise ValueError

        client_appointments = found_clients[0].get('appointments')
        if client_appointments is None:
            client_appointments = []
        result_appointments = []
        for ca in client_appointments:
            if datetime_start <= ca['start'] and ca['end'] <= datetime_end:
                result_appointments.append(ca)

        result = [{'master_phone': i['master'],
                   'client_phone': client_phone,
                   'time_interval': {
                       'dt_start': i['start'],
                       'dt_end': i['end']
                   }} for i in result_appointments]

        return {'result': result}
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/master/remove_appointment', methods=['POST'])
def master_remove_appointment():
    error_msg = 'Error occurred'
    try:
        data = request.get_json()
        print(data)
        master_phone = data.get('master_phone')
        client_phone = data.get('client_phone')
        time_interval = data.get('time_interval')

        if time_interval is None or master_phone is None or client_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        dt_start = time_interval.get('dt_start')
        dt_end = time_interval.get('dt_end')

        if dt_start is None or dt_end is None:
            error_msg = 'Please enter working hours interval as required'
            raise ValueError

        datetime_start = dt_to_datetime(dt_start)
        datetime_end = dt_to_datetime(dt_end)

        if datetime_start >= datetime_end:
            error_msg = 'Start of time interval should be earlier than it`s end'
            raise ValueError

        search_masters = master_coll.find({'phone': master_phone})
        found_masters = [i for i in search_masters]

        if len(found_masters) == 0:
            error_msg = f'Master with phone"{master_phone}" has not been found in db'
            raise ValueError

        search_clients = client_coll.find({'phone': client_phone})
        found_clients = [i for i in search_clients]

        if len(found_clients) == 0:
            error_msg = f'Client with phone"{client_phone}" has not been found in db'
            raise ValueError

        client_appointments = found_clients[0].get('appointments')
        client_id = found_clients[0]['_id']
        master_appointments = found_masters[0].get('appointments')
        master_id = found_masters[0]['_id']
        if client_appointments is None:
            client_appointments = []
        if master_appointments is None:
            master_appointments = []
        client_appointment_existed = False
        master_appointment_existed = False
        for ca in client_appointments:
            if ca['start'] == datetime_start and ca['end'] == datetime_end and ca['master'] == master_phone:
                client_appointments.remove(ca)
                client_appointment_existed = True
                break
        for ma in master_appointments:
            if ma['start'] == datetime_start and ma['end'] == datetime_end and ma['client'] == client_phone:
                master_appointments.remove(ma)
                master_appointment_existed = True
                break

        if not client_appointment_existed and not master_appointment_existed:
            error_msg = 'Appointment does not exist'
            raise ValueError

        if not client_appointment_existed:
            error_msg = 'Client with corresponding appointment could not be found in db'
            raise ValueError

        if not master_appointment_existed:
            error_msg = 'Master with corresponding appointment could not be found in db'
            raise ValueError

        client_coll.update({'_id': client_id},
                           {'$set': {
                               'appointments': client_appointments
                           }}, upsert=False)

        master_coll.update({'_id': master_id},
                           {'$set': {
                               'appointments': master_appointments
                           }}, upsert=False)

        return f'Successfully removed an appointment for master with phone "{master_phone}" and client with phone "{client_phone}"'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/client/reg', methods=['POST'])
def client_reg():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        # name = data.get('name')
        phone = data.get('phone')

        if phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_results = client_coll.find({'phone': phone})
        search_docs = [i for i in search_results]
        if len(search_docs) > 0:
            error_msg = f'Client with phone "{phone}" is already registered'
            raise ValueError

        post = data
        client_coll.insert_one(post)
        return 'Successfully added a new client to db'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/client/del', methods=['POST'])
def client_del():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        phone = data.get('phone')

        if phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_clients = client_coll.find({'phone': phone})
        found_clients = [i for i in search_clients]
        if len(found_clients) == 0:
            error_msg = f'Client with phone "{phone}" not found in db'
            raise ValueError

        client_appointments = found_clients[0].get('appointments')
        if client_appointments is None:
            client_appointments = []
        current_datetime = datetime.now()
        for appointment in client_appointments:
            if appointment['start'] > current_datetime:
                search_masters = master_coll.find({'phone': appointment['master']})
                found_masters = [i for i in search_masters]
                master_id = found_masters[0]['_id']
                print(found_masters[0]['phone'])
                master_appointments = found_masters[0]['appointments']
                for master_appointment in master_appointments:
                    if master_appointment['start'] == appointment['start'] and \
                            master_appointment['end'] == appointment['end'] and \
                            master_appointment['client'] == phone:
                        master_appointments.remove(master_appointment)
                        break
                master_coll.update({'_id': master_id},
                                   {'$set': {
                                       'appointments': master_appointments
                                   }}, upsert=False)

        client_id = found_clients[0]['_id']
        print(client_id)
        client_coll.delete_one({'_id': client_id})

        return 'Successfully removed client from db'
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


@app.route(f'{BASE_URL}/client/get_profile', methods=['POST'])
def client_get_profile():
    error_msg = 'Error occurred'
    # error = False
    try:
        data = request.get_json()
        print(data)
        client_phone = data.get('phone')

        if client_phone is None:
            error_msg = 'Please enter required parameters'
            raise ValueError

        search_clients = client_coll.find({'phone': client_phone})
        found_clients = [i for i in search_clients]
        if len(found_clients) == 0:
            error_msg = f'Client with phone "{client_phone}" does not exist in db'
            raise ValueError

        del found_clients[0]['_id']
        return {'result': found_clients[0]}
    except Exception as e:
        print(e)
        return {'message': error_msg}, 400


if __name__ == '__main__':
    app.run(debug=True, port=5001)
