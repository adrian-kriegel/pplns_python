
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
  Worker

ApiScheme = typing.Literal['http', 'https']

class PipelineApi:

  endpoint : UrlParseResult

  def __init__(
    self,
    base_url : str
  ) -> None:

    self.endpoint = urlparse(base_url)


  def build_uri(
    self,
    path : str,
    query : dict[str, typing.Any] = {}
  ) -> str:

    return urlunsplit(
      (
        self.endpoint.scheme,
        self.endpoint.netloc,
        os.path.join(self.endpoint.path, path),
        urlencode(query),
        ""
      )
    )

  def build_request(
    self,
    url : str,
    body : typing.Any # TODO: type
  ):

    return {
      'url': url,
      'headers': { 'Content-Type': 'application/json' }, 
      'data': json.dumps(body)
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
