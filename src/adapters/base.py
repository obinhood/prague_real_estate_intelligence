from abc import ABC, abstractmethod


class BaseAdapter(ABC):
    source_name = "base"

    @abstractmethod
    def scrape(self):
        raise NotImplementedError
