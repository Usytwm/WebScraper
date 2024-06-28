import time
from itertools import takewhile
import operator
from collections import OrderedDict
from abc import abstractmethod, ABC


class IStorage(ABC):
    @abstractmethod
    def __setitem__(self, key, value):
        # Almacena un valor con la clave dada
        pass

    @abstractmethod
    def __getitem__(self, key):
        # Devuelve el valor almacenado con la clave dada
        pass

    @abstractmethod
    def get(self, key, default=None):
        # Devuelve el valor almacenado con la clave dada o un valor por defecto
        pass

    @abstractmethod
    def iter_older_than(self, seconds_old):
        # Itera sobre los elementos más antiguos que el tiempo especificado
        pass

    @abstractmethod
    def __iter__(self):
        # Itera sobre todos los elementos almacenados
        pass


class ForgetfulStorage(IStorage):
    def __init__(self, ttl=604800):
        # Inicializa el almacenamiento con un tiempo de vida (TTL)
        self.data = OrderedDict()
        self.ttl = ttl

    def __setitem__(self, key, value):
        # Almacena un valor y elimina el antiguo si existe
        if key in self.data:
            del self.data[key]
        self.data[key] = (time.monotonic(), value)
        self.cull()

    def cull(self):
        # Elimina los elementos que han superado el TTL
        for _, _ in self.iter_older_than(self.ttl):
            self.data.popitem(last=False)

    def get(self, key, default=None):
        # Devuelve un valor almacenado o un valor por defecto
        self.cull()
        if key in self.data:
            return self[key]
        return default

    def __getitem__(self, key):
        # Devuelve un valor almacenado con la clave dada
        self.cull()
        return self.data[key][1]

    def __repr__(self):
        self.cull()
        return repr(self.data)

    def iter_older_than(self, seconds_old):
        # Itera sobre los elementos más antiguos que el tiempo especificado
        min_birthday = time.monotonic() - seconds_old
        zipped = self._triple_iter()
        matches = takewhile(lambda r: min_birthday >= r[1], zipped)
        return list(map(operator.itemgetter(0, 2), matches))

    def _triple_iter(self):
        # Devuelve un iterador de claves, tiempos de vida y valores
        ikeys = self.data.keys()
        ibirthday = map(operator.itemgetter(0), self.data.values())
        ivalues = map(operator.itemgetter(1), self.data.values())
        return zip(ikeys, ibirthday, ivalues)

    def __iter__(self):
        self.cull()
        ikeys = self.data.keys()
        ivalues = map(operator.itemgetter(1), self.data.values())
        return zip(ikeys, ivalues)
