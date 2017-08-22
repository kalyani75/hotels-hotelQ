import os
import json
import sys

import redis
import requests

from flask import Blueprint, jsonify, request, url_for, make_response, abort
from flask_cors import cross_origin

from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
hotel = Blueprint('hotel', __name__)

redispool = None
mqpool = None

if 'VCAP_SERVICES' in os.environ: 
  vcap_services = json.loads(os.environ['VCAP_SERVICES'])

  uri = ''
  urimq = ''

  for key, value in vcap_services.iteritems():   # iter on both keys and values
    if key.find('redis') > 0:
      redis_info = vcap_services[key][0]
		
      cred = redis_info['credentials']
      uri = cred['uri'].encode('utf8')
      urimq = cred['uri'].encode('utf8')

  redispool = redis.ConnectionPool.from_url(uri + '/0')
  mqpool  = redis.ConnectionPool.from_url(urimq + '/0')  
else:
  redispool = redis.ConnectionPool(host=os.getenv('REDIS_HOST', 'localhost'), port=os.getenv('REDIS_PORT', 6379), db=0)
  mqpool = redis.ConnectionPool(host=os.getenv('MQ_HOST', 'localhost'), port=os.getenv('MQ_PORT', 6379), db=0)

PORT = int(os.getenv('PORT', '9012'))
ZIPKINHOST = os.getenv('ZIPKINHOST', 'zipkin')
ZIPKINPORT = int(os.getenv('ZIPKINPORT', 9411))
ZIPKINSAMPLERATE = float(os.getenv('ZIPKINSAMPLERATE', 100.0))

@hotel.errorhandler(400)
def not_found(error):
  return make_response(jsonify( { 'error': 'Bad request' }), 400)

@hotel.errorhandler(404)
def not_found(error):
  return make_response(jsonify( { 'error': 'Not found' }), 404)

def http_transport(encoded_span):
  # The collector expects a thrift-encoded list of spans.
  requests.post('http://' + ZIPKINHOST + ':' + str(ZIPKINPORT) + '/api/v1/spans', data=encoded_span, headers={'Content-Type': 'application/x-thrift'})  

@zipkin_span(service_name='hotels.com:hotelquery', span_name='hotelquery:gethotelfragments')
def gethotelfragments(prefix, pagelength):
  rdb = redis.StrictRedis(connection_pool=redispool)

  prefix = prefix.lower()
  listpart = 50

  start = rdb.zrank('hotelfragments', prefix)
  if start < 0: return []

  hotelarray = []
  while (len(hotelarray) != pagelength):
    range = rdb.zrange('hotelfragments', start, start + listpart - 1)
    start += listpart

    if not range or len(range) <= 0: 
      break

    for entry in range:
      minlen = min(len(entry), len(prefix))
      
      if entry[0:minlen] != prefix[0:minlen]:
        pagelength = len(hotelarray)
        break

      if entry[-1] == '%' and len(hotelarray) != pagelength: 
        hotel = {}
        
        hotelfull = entry[0:-1]
        indexwithperc = hotelfull.rfind('%')

        hotelid = entry[indexwithperc + 1:-1]
        hotelname = entry[0:indexwithperc] 
        
        hotelproperties = rdb.lrange(hotelid, 0, -1)
        if len(hotelproperties) > 0:
          hotel['id'] = hotelproperties[0]
          hotel['displayname'] = hotelproperties[1]
          hotel['acname'] = hotelproperties[2]
          hotel['image'] = hotelproperties[3]
          hotel['latitude'] = hotelproperties[4]
          hotel['longitude'] = hotelproperties[5]
          hotel['thirdpartyrating'] = hotelproperties[6]
        
          hotelarray.append(hotel)

  return hotelarray

@hotel.route('/api/v1.0/hotels/autocomplete/<prefix>', methods=['GET'])
@cross_origin()
def autocomplete(prefix):
  with zipkin_span(service_name='hotels.com:hotelquery', span_name='hotelquery:autocomplete', transport_handler=http_transport,
    port=PORT, sample_rate=ZIPKINSAMPLERATE):  
    if request.args.get('pagelength') is None: pagelength = 20
    else: pagelength = int(request.args.get('pagelength'))

    hotelarray = gethotelfragments(prefix, pagelength)

    hotelcollection = {}
    hotelcollection['hotels'] = hotelarray

    return json.dumps(hotelcollection)  

@hotel.route('/api/v1.0/hotels', methods=['POST'])
@cross_origin()
def createhotel():
  with zipkin_span(service_name='hotels.com:hotelquery', span_name='hotelquery:createhotel', transport_handler=http_transport,
    port=PORT, sample_rate=ZIPKINSAMPLERATE):  
    rdb = redis.StrictRedis(connection_pool=redispool)
    if not request.json or not 'displayname' in request.json or not 'id' in request.json:
      abort(400)
 
    hotel = {
      'id': request.json['id'],
      'displayname': request.json['displayname'],
      'acname': request.json['acname'],    
      'image': request.json.get('image', ''),
      'latitude': request.json.get('latitude', 0),
      'longitude': request.json.get('longitude', 0),
      'thirdpartyrating': request.json.get('thirdpartyrating', 0)
    }

    hotelname = hotel['acname']
    for l in range(1, len(hotelname)):
      hotelfragment = hotelname[0:l]
      rdb.zadd('hotelfragments', 0, hotelfragment)
  
    hotelwithid = hotelname + '%H-' + str(hotel['id']) + '%'
    rdb.zadd('hotelfragments', 0, hotelwithid)  
  
    hotelkey = 'H-' + str(hotel['id'])
    rdb.execute_command('geoadd', 'hotels', '%f' % hotel['longitude'], '%f' % hotel['latitude'], hotelkey)
    rdb.delete(hotelkey)

    rdb.rpush(hotelkey, hotel['id'])
    rdb.rpush(hotelkey, hotel['displayname'])
    rdb.rpush(hotelkey, hotel['acname'])
    rdb.rpush(hotelkey, hotel['image'])
    rdb.rpush(hotelkey, hotel['latitude'])
    rdb.rpush(hotelkey, hotel['longitude'])  
    rdb.rpush(hotelkey, hotel['thirdpartyrating']) 
  
    return jsonify({ 'hotel': hotel }), 201 

@hotel.route('/api/v1.0/hotels/search/<float:latitude>/<float:longitude>', methods=['GET'])
@cross_origin()
def hotelsearchbydistance(latitude, longitude):
  with zipkin_span(service_name='hotels.com:hotelquery', zipkin_attrs=ZipkinAttrs(trace_id=request.headers['X-B3-TraceID'],
    span_id=request.headers['X-B3-SpanID'], parent_span_id=request.headers['X-B3-ParentSpanID'], flags='1', is_sampled=
    request.headers['X-B3-Sampled']), span_name='hotelquery:hotelsearchbydistance', transport_handler=http_transport, port=PORT, 
    sample_rate=ZIPKINSAMPLERATE):
    sessionid = request.args.get('sessionid')

    mq = redis.StrictRedis(connection_pool=mqpool)
    mq.publish(sessionid, json.dumps({ 'sessionid': sessionid, 'event': 'hotelsearchinitiated' }))

    rdb = redis.StrictRedis(connection_pool=redispool)
    radius = int(request.args.get('radius'))

    hotels = []
    searchids = []

    searchresults = rdb.execute_command('georadius', 'hotels', '%f' % longitude, '%f' % latitude, '%d' % radius, 'km', 'WITHDIST', 'ASC')
    for hotel in searchresults:
      hotelproperties = rdb.lrange(hotel[0], 0, -1)  

      eachhotel = {}
      eachhotel['id'] = hotelproperties[0]
      eachhotel['displayname'] = hotelproperties[1]
      eachhotel['acname'] = hotelproperties[2]
      eachhotel['image'] = hotelproperties[3]
      eachhotel['latitude'] = hotelproperties[4]
      eachhotel['longitude'] = hotelproperties[5]
      eachhotel['distance'] = round(float(hotel[1]), 3)
      eachhotel['thirdpartyrating'] = int(hotelproperties[6])
        
      searchids.append(int(eachhotel['id']))
      hotels.append(eachhotel)

    mq.publish(sessionid, json.dumps({ 'sessionid': sessionid, 'event': 'hotelsearchcompleted', 'searchids': searchids }))
    return jsonify({ 'hotelsearch': hotels })  