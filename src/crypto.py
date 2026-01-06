import hashlib
import coincurve
from typing import List, Optional
from bitarray import bitarray

def get_pubkey_from_input(script_sig: bytes, witness: List[str]) -> Optional[bytes]:
    if len(witness) == 2:
        try:
            pk = bytes.fromhex(witness[1])
            if len(pk) == 33:
                return pk
        except:
            pass

    if script_sig:
        if len(script_sig) >= 34:
             candidate = script_sig[-33:]
             try:
                 coincurve.PublicKey(candidate)
                 return candidate
             except:
                 pass

    return None

def compute_tweak(pubkeys: List[bytes]) -> bytes:
    if not pubkeys:
        return b''

    point = None
    for pk_bytes in pubkeys:
        try:
            pk = coincurve.PublicKey(pk_bytes)
            if point is None:
                point = pk
            else:
                point = coincurve.PublicKey.combine_keys([point, pk])
        except Exception:
            continue

    if point:
        return point.format(compressed=True)
    return b''

def siphash(k: bytes, m: bytes) -> int:
    return _siphash24(k, m)

def _siphash24(k: bytes, m: bytes) -> int:
    def rotl(x, b):
        return ((x << b) | (x >> (64 - b))) & 0xffffffffffffffff

    k0 = int.from_bytes(k[0:8], 'little')
    k1 = int.from_bytes(k[8:16], 'little')
    
    v0 = k0 ^ 0x736f6d6570736575
    v1 = k1 ^ 0x646f72616e646f6d
    v2 = k0 ^ 0x6c7967656e657261
    v3 = k1 ^ 0x7465646279746573
    
    for i in range(0, len(m) // 8):
        mi = int.from_bytes(m[i*8:(i+1)*8], 'little')
        v3 ^= mi
        
        for _ in range(2):
            v0 = (v0 + v1) & 0xffffffffffffffff
            v2 = (v2 + v3) & 0xffffffffffffffff
            v1 = rotl(v1, 13)
            v3 = rotl(v3, 16)
            v1 ^= v0
            v3 ^= v2
            v0 = rotl(v0, 32)
            v2 = (v2 + v1) & 0xffffffffffffffff
            v0 = (v0 + v3) & 0xffffffffffffffff
            v1 = rotl(v1, 17)
            v3 = rotl(v3, 21)
            v1 ^= v2
            v3 ^= v0
        
        v0 ^= mi

    last = m[len(m)//8*8:]
    last_block = len(m) << 56
    for i, b in enumerate(last):
        last_block |= b << (8 * i)
    
    v3 ^= last_block
    
    for _ in range(2):
        v0 = (v0 + v1) & 0xffffffffffffffff
        v2 = (v2 + v3) & 0xffffffffffffffff
        v1 = rotl(v1, 13)
        v3 = rotl(v3, 16)
        v1 ^= v0
        v3 ^= v2
        v0 = rotl(v0, 32)
        v2 = (v2 + v1) & 0xffffffffffffffff
        v0 = (v0 + v3) & 0xffffffffffffffff
        v1 = rotl(v1, 17)
        v3 = rotl(v3, 21)
        v1 ^= v2
        v3 ^= v0

    v2 ^= 0xff
    
    for _ in range(4):
        v0 = (v0 + v1) & 0xffffffffffffffff
        v2 = (v2 + v3) & 0xffffffffffffffff
        v1 = rotl(v1, 13)
        v3 = rotl(v3, 16)
        v1 ^= v0
        v3 ^= v2
        v0 = rotl(v0, 32)
        v2 = (v2 + v1) & 0xffffffffffffffff
        v0 = (v0 + v3) & 0xffffffffffffffff
        v1 = rotl(v1, 17)
        v3 = rotl(v3, 21)
        v1 ^= v2
        v3 ^= v0

    return (v0 ^ v1 ^ v2 ^ v3)

def golomb_rice_encode(values: List[int], P: int, M: int) -> bytes:
    if not values:
        return b'\x00'

    diffs = []
    last = 0
    for v in values:
        diffs.append(v - last)
        last = v
        
    ba = bitarray()
    for d in diffs:
        quotient = d >> P
        remainder = d & ((1 << P) - 1)
        
        ba.extend([0] * quotient)
        ba.append(1)
        
        for i in range(P):
            ba.append((remainder >> i) & 1) 
            
    return ba.tobytes()

def create_block_filter(block_hash_hex: str, scripts: List[bytes]) -> bytes:
    if not scripts:
        return b'\x00'

    block_hash = bytes.fromhex(block_hash_hex)
    k = block_hash[:16]
    
    M = 784931
    P = 19
    
    N = len(scripts)
    F = N * M
    
    values = []
    for s in scripts:
        v = siphash(k, s)
        values.append(v % F)
        
    values.sort()
    
    return golomb_rice_encode(values, P, F)
