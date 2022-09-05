
import typing

from pplns_types import \
  DataItem, \
  DataItemWrite, \
  FlowIdSchema


PreparedInput = typing.TypedDict(
  'PreparedInput',
  {
    # bundle id
    '_id': str,
    # bundle taskId
    'taskId': str,
    # bundle flowId
    'flowId': FlowIdSchema,
    # id of the consumer node
    'consumerId': str,
    # data items by their name
    'inputs': dict[str, DataItem],
  },
)

class BatchProcessor:

  max_batch_size : int = 50

  def __call__(self, inputs : list[PreparedInput]) -> list[DataItemWrite] | None:

    raise Exception('Not implemented.')

BundleProcessor = typing.Callable[
  [PreparedInput],
  DataItemWrite | None
] | BatchProcessor
