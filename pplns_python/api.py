
import os.path

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

  def register_worker(
    self,
    worker : WorkerWrite
  ) -> Worker:

    result = requests.post(
      self.build_uri('/workers'),
      data=worker
    )

    return result.json()
