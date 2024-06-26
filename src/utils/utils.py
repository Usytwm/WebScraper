import hashlib
import operator
import asyncio


async def gather_dict(dic):
    # Recopila los resultados de un diccionario de corutinas
    cors = list(dic.values())
    results = await asyncio.gather(*cors)
    return dict(zip(dic.keys(), results))


def digest(string):
    # Calcula el hash SHA-1 de una cadena y devuelve su digest
    if not isinstance(string, bytes):
        string = str(string).encode("utf8")
    return hashlib.sha1(string).digest()


def shared_prefix(args):
    # Encuentra el prefijo compartido más largo entre varias cadenas
    i = 0
    while i < min(map(len, args)):
        if len(set(map(operator.itemgetter(i), args))) != 1:
            break
        i += 1
    return args[0][:i]


def bytes_to_bit_string(bites):
    # Convierte una secuencia de bytes en una cadena de bits
    bits = [bin(bite)[2:].rjust(8, "0") for bite in bites]
    return "".join(bits)
