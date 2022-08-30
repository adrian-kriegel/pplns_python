
from urllib.parse import urlparse

from pplns_python.api import PipelineApi

def test_build_uri():

  uri = PipelineApi('http://example.com/api').build_uri(
    'path/to/resource',
    { 'foo': 'bar', 'test': 'bart' }
  )

  parsed = urlparse(uri)

  assert parsed.scheme == 'http'
  assert parsed.netloc == 'example.com'
  assert parsed.path == '/api/path/to/resource'
  assert parsed.query == 'foo=bar&test=bart'

  assert uri == 'http://example.com/api/path/to/resource?foo=bar&test=bart'
