
import typing
from typing_extensions import NotRequired
from xmlrpc.client import boolean

from pplns_types import \
  DataItem, \
  BundleRead, \
  FlowIdSchema


PreparedInput = typing.TypedDict(
  'PreparedInput',
  {
    # bundle id
    '_id': str,
    # bundle taskId
    'taskId': str,
    # id of the consumer node
    'consumerId': str,
    # data items by their name
    'inputs': dict[str, DataItem],
    # original bundle
    'bundle': BundleRead
  },
)

# partial data item to emit from a single output channel
OutputPerChannel = typing.TypedDict(
  'OutputPerChannel',
  {
    'done': NotRequired[boolean],
    'data': list[typing.Any],
  }
)

# maps output channel name to partial DataItem
ProcessorOutput = dict[str, OutputPerChannel]

class BatchProcessor:

  max_batch_size : int = 50

  def __call__(self, inputs : list[PreparedInput]) -> list[ProcessorOutput] | None:

    raise Exception('Not implemented.')

BundleProcessor = typing.Callable[
  [PreparedInput],
  ProcessorOutput | None
] | BatchProcessor
