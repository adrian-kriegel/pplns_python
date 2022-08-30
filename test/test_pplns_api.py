
import os
from urllib.parse import ParseResult, urlparse

from pplns_python.api import PipelineApi
from pplns_python.example_worker import example_worker

def env(s : str) -> str:

  v: str | None  = os.environ.get(s)

  if v:
    return v
  else:
    raise Exception('Missing environment variable: ' + s)

def test_build_uri() -> None:

  uri: str = PipelineApi('http://example.com/api').build_uri(
    'path/to/resource',
    { 'foo': 'bar', 'test': 'bart' }
  )

  parsed: ParseResult = urlparse(uri)

  assert parsed.scheme == 'http'
  assert parsed.netloc == 'example.com'
  assert parsed.path == '/api/path/to/resource'
  assert parsed.query == 'foo=bar&test=bart'

  assert uri == 'http://example.com/api/path/to/resource?foo=bar&test=bart'

def test_register_worker():

  api = PipelineApi(env('PPLNS_API'))

  result = api.register_worker(
    example_worker
  )

  assert result['_id']