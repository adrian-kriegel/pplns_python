
import requests

from urllib.parse import urlunsplit, urlencode

import typing

from pplns_types import \
  WorkerWrite, \
  Worker

class PipelineApi:

  endpoint : str
  scheme : str

  def __init__(
    self,
    endpoint : str,
    scheme : typing.Literal['http', 'https']
  ) -> None:

    self.endpoint = endpoint
    self.scheme = scheme

  def build_uri(
    self,
    path : str,
    query : dict[str, typing.Any] = {}
  ) -> str:

    return urlunsplit(
      (
        self.scheme,
        self.endpoint,
        path,
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
