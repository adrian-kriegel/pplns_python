
import os.path

import json
import typing 

import requests

from urllib.parse import\
  urlunsplit, \
  urlencode, \
  urlparse, \
  ParseResult as UrlParseResult

import typing

from pplns_types import \
  WorkerWrite, \
  Worker, \
  BundleRead, \
  BundleQuery, \
  DataItemWrite, \
  DataItemQuery

from pplns_python.input_stream import BundleProcessor, InputStream



def stringify_value(value : typing.Any) -> str:

  '''
  Stringifies values for use in a query string.
  '''

  return value if isinstance(value, str) else json.dumps(value)

def clean_query(query : typing.Any):

  '''
  Removes all Nones from the query and turns True/False into true/false.
  '''

  return {
    key: stringify_value(value) 
    for key, value in query.items() 
    if not value == None
  }

class PipelineApi:

  __endpoint : UrlParseResult

  workers : dict[str, Worker]

  def __init__(
    self,
    base_url : str
  ) -> None:

    self.__endpoint = urlparse(base_url)

    self.workers = {}

  def get(self, **request_params) -> typing.Any:

    return self.__parse_response(requests.get(**request_params))

  def post(self, **request_params) -> typing.Any:

    return self.__parse_response(requests.post(**request_params))

  def put(self, **request_params) -> typing.Any:

    return self.__parse_response(requests.put(**request_params))

  def delete(self, **request_params) -> typing.Any:

    return self.__parse_response(requests.delete(**request_params))

  def patch(self, **request_params) -> typing.Any:

    return self.__parse_response(requests.patch(**request_params))

  def __parse_response(
    self,
    response : requests.Response
  ) -> dict:

    is_json: bool = response.headers['Content-Type'].startswith('application/json')

    body: typing.Any | None = response.json() if is_json else None
    
    if (
      response.status_code >= 200 and 
      response.status_code < 300 and 
      body
    ):

      return body

    else:

      if is_json:
        
        raise Exception(
          'API error:\n' +
          json.dumps(body, indent=4)
        )

      else:

        raise Exception('Unknown API error: \n' + response.text)

  def build_uri(
    self,
    path : str,
    query : typing.Any = {} # TODO: typing
  ) -> str:

    return urlunsplit(
      (
        self.__endpoint.scheme,
        self.__endpoint.netloc,
        os.path.join(self.__endpoint.path, path),
        urlencode(clean_query(query)),
        ""
      )
    )

  def build_request(
    self,
    url : str | tuple[str, typing.Any],
    body : typing.Any = None # TODO: type
  ):

    return {
      'url': self.build_uri(url) if isinstance(url, str) else self.build_uri(url[0], url[1]),
      'headers': { 'Content-Type': 'application/json' }, 
      'data': json.dumps(body) if body else None
    }

  def register_worker(
    self,
    worker : WorkerWrite
  ) -> Worker:

    params = self.build_request(
      '/workers',
      worker
    )

    worker_read : Worker = self.post(**params)

    self.workers[worker_read['_id']] = worker_read

    return worker_read


  def consume(
    self,
    query : BundleQuery
  ) -> list[BundleRead]:

    params = self.build_request(
      ('/bundles', query),
    )

    get_response = self.get(**params)
    
    return get_response['results']

  def unconsume(
    self,
    bundle_id : str
  ):

    return self.put(
      url=('/bundles/' + bundle_id)
    )

  def emit_item(
    self,
    query : DataItemQuery,
    item : DataItemWrite
  ):
    
    return self.post(
      **self.build_request(
        ('/outputs', query),
        item,
      )
    )

  
  def on_bundle(
    self,
    query : BundleQuery,
    processor : BundleProcessor,
    **input_stream_args
  ):

    '''
    Initializes InputStream to watch for new bundles with that match the provided query.
    '''

    stream = InputStream(
      self,
      query,
      **input_stream_args
    )

    stream.on('data', processor)

    return stream