import abc
from typing import Generic, Optional, TypeVar

from stupidb.typehints import Result

Aggregate = TypeVar("Aggregate", covariant=True)


class Aggregator(Generic[Aggregate, Result], abc.ABC):
    @abc.abstractmethod
    def query(self, begin: int, end: int) -> Optional[Result]:
        ...
