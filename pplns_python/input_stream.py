
import threading
from time import time
import typing

from pplns_types import \
  BundleQuery, \
  BundleRead, \
  DataItemWrite

from pplns_python.api import PipelineApi

BundleProcessor = typing.Callable[
  [BundleRead],
  list[DataItemWrite] | DataItemWrite
]

class Stream:

  handlers : dict[str, list[typing.Callable]]

  def on(
    self,
    event : str,
    handler : typing.Callable
  ):

    if not event in self.handlers:
      self.handlers[event] = []

    self.handlers[event].append(handler)

    return self

  def emit(
    self,
    event : str,
    *args : typing.Any
  ) -> None:
  
    if (not event in self.handlers):
      return

    for fnc in self.handlers[event]:

      fnc(*args)

class Interval:

  def __init__(self, interval, action) -> None:

    self.interval=interval
    self.action=action
    self.stopEvent=threading.Event()
    thread=threading.Thread(target=self.__setInterval)
    thread.start()

  def __setInterval(self) -> None:

    nextTime=time.time()+self.interval

    while not self.stopEvent.wait(nextTime-time.time()):

        nextTime+=self.interval
        self.action()

  def cancel(self)  -> None:

    self.stopEvent.set()

class InputStream(Stream):

  '''
  Emits 'data' event once there are new data bundles to be consumed from the api.
  '''

  query : BundleQuery
  max_concurrency : int
  polling_time : int

  active_callbacks : int = 0

  interval : Interval | None = None

  def __init__(
    self,
    api : PipelineApi,
    query : BundleQuery,
    max_concurrency : int = 10,
    polling_time : int = 500,
  ) -> None:

    self.api: PipelineApi = api
    self.query = query
    self.max_concurrency = max_concurrency
    self.polling_time = polling_time

  def on(
    self,
    event : str,
    callback : typing.Callable
  ) -> Stream:

    '''
    Same as Stream.on but wraps 'data' callbacks in InputStreamDataCallback.
    If the callback raises an Exception, the bundle will be put back to into unconsumed bundles collection.
    '''

    if event == 'data':

      if 'data' in self.handlers:

        raise Exception('InputStream can only have one data callback.')

      else:

        return Stream.on(
          self, 
          event,
          InputStreamDataCallback(self, callback)
        )


    else:

      return Stream.on(self, event, callback)

  def pause(self):

    ''' Pauses stream. '''

    if self.interval:
      self.interval.cancel()
      self.interval = None

  def start(self) -> None:

    ''' Starts stream if not already started. '''

    if not self.interval:
      
      self.interval = Interval(self.polling_time, self.__poll)

  def resume(self) -> None:

    ''' Resumes or stars stream. '''

    return self.start()

  def __poll(self) -> None:

    '''
    Runs one single polling iteration.
    '''

  def __handle_callback_error(
    self,
    bundle : BundleRead,
    error : Exception
  ) -> None:

    self.api.unconsume(bundle['_id'])

    self.emit('error', error)


class InputStreamDataCallback:

  '''
  Wraps a 'data' callback for an InputStream.
  '''

  def __init__(
    self,
    stream : InputStream,
    fnc : typing.Callable[[BundleRead], typing.Any]
  ) -> None:

    self.stream: InputStream = stream
    self.fnc = fnc

  def __call__(self, bundle : BundleRead) -> None:

    try:

      if ++self.stream.active_callbacks > self.stream.max_concurrency:

        self.stream.pause()

      self.fnc(bundle)

    except Exception as e:

      self.stream.__handle_callback_error(bundle, e)

    finally:

      self.stream.active_callbacks -= 1

      if self.stream.active_callbacks <= self.stream.max_concurrency:

        self.stream.resume()
