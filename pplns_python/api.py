
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
  DataItemQuery, \
  DataItem

from pplns_python.stream import InputStream

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
  
  client = requests

  task_id : str | None

  def __init__(
    self,
    base_url : str
  ) -> None:

    self.__endpoint = urlparse(base_url)

    self.workers = {}

  def get(self, **request_params) -> typing.Any:

    return self.__parse_response(self.client.get(**request_params))

  def post(self, **request_params) -> typing.Any:

    return self.__parse_response(self.client.post(**request_params))

  def put(self, **request_params) -> typing.Any:

    return self.__parse_response(self.client.put(**request_params))

  def delete(self, **request_params) -> typing.Any:

    return self.__parse_response(self.client.delete(**request_params))

  def patch(self, **request_params) -> typing.Any:

    return self.__parse_response(self.client.patch(**request_params))

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

  def get_registered_worker(self, workerId : typing.Optional[str]) -> Worker:

    '''
    Finds a worker that has been registered using register_worker.
    '''

    if not workerId or not workerId in self.workers:
      raise Exception(f'Worker {workerId} has not been registered locally.')

    return self.workers[workerId]

  def consume(
    self,
    query : BundleQuery 
  ) -> list[BundleRead]:

    '''
    Same as get_bundles(...) with consume=True by default
    '''

    return self.get_bundles(
      { 'consume': True, **query }
    )

  def get_bundles(
    self,
    query : BundleQuery
  ) -> list[BundleRead]:

    '''
    Returns input bundles for the given query.
    '''

    params = self.build_request(
      ('/bundles', query),
    )

    get_response = self.get(**params)
    
    return get_response['results']

  def unconsume(
    self,
    task_id : str,
    bundle_id : str
  ) -> None:

    '''
    Undo consuming a bundle.
    '''

    return self.put(
      **self.build_request(f'/tasks/{task_id}/bundles/{bundle_id}')
    )

  def emit_item(
    self,
    query : DataItemQuery,
    item : DataItemWrite
  ) -> DataItem:
    
    '''
    Emit a DataItem as an output.
    '''

    return self.post(
      **self.build_request(
        ('/outputs', query),
        item,
      )
    )

  
  def create_input_stream(
    self,
    query : BundleQuery,
    **input_stream_args
  ) -> InputStream:

    '''
    Initializes InputStream to watch for new bundles that match the provided query.
    '''

    return InputStream(
      self,
      query,
      **input_stream_args
    )