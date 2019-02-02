import abc
from typing import Generic, Optional, TypeVar

from stupidb.typehints import Result

AggregateClass = TypeVar("AggregateClass", covariant=True)


class Aggregator(Generic[AggregateClass, Result], abc.ABC):
    @abc.abstractmethod
    def query(self, begin: int, end: int) -> Optional[Result]:
        ...
