
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
  BundleQuery

from pplns_python.input_stream import BundleProcessor, InputStream


class PipelineApi:

  __endpoint : UrlParseResult

  def __init__(
    self,
    base_url : str
  ) -> None:

    self.__endpoint = urlparse(base_url)


  def build_uri(
    self,
    path : str,
    query : dict[str, typing.Any] = {}
  ) -> str:

    return urlunsplit(
      (
        self.__endpoint.scheme,
        self.__endpoint.netloc,
        os.path.join(self.__endpoint.path, path),
        urlencode(query),
        ""
      )
    )

  def build_request(
    self,
    url : str,
    body : typing.Any = None # TODO: type
  ):

    return {
      'url': url,
      'headers': { 'Content-Type': 'application/json' }, 
      'data': json.dumps(body) if body else None
    }

  def register_worker(
    self,
    worker : WorkerWrite
  ) -> Worker:

    params = self.build_request(
      self.build_uri('/workers'),
      worker
    )

    result = requests.post(**params)

    return result.json()

  def consume(
    self,
    **query : BundleQuery
  ) -> list[BundleRead]:

    params = self.build_request(
      self.build_uri('/bundles', query),
    )

    get_response = requests.get(**params).json()

    return get_response['results']

  def unconsume(
    self,
    bundle_id : str
  ):

    return requests.put(
      self.build_uri('/bundles/' + bundle_id)
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