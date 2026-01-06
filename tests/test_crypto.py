import pytest
import coincurve
from src.crypto import get_pubkey_from_input, compute_tweak, create_block_filter, siphash

def test_siphash():
    # Test vector from somewhere or just consistency
    k = b'\x00' * 16
    m = b'hello'
    # We just want to ensure it runs and returns an int
    res = siphash(k, m)
    assert isinstance(res, int)

def test_compute_tweak():
    # 1. Generate some keys
    priv1 = coincurve.PrivateKey()
    priv2 = coincurve.PrivateKey()
    
    pub1 = priv1.public_key.format(compressed=True)
    pub2 = priv2.public_key.format(compressed=True)
    
    # 2. Compute tweak
    tweak = compute_tweak([pub1, pub2])
    
    # 3. Verify manual addition
    sum_point = coincurve.PublicKey(pub1).combine_keys([coincurve.PublicKey(pub1), coincurve.PublicKey(pub2)])
    expected = sum_point.format(compressed=True)
    
    assert tweak == expected
    assert len(tweak) == 33

def test_get_pubkey_p2wpkh():
    # Mock witness for P2WPKH: [signature, pubkey]
    pubkey_hex = "03" + "00"*32 # Fake compressed key
    witness = ["sig", pubkey_hex]
    
    pk = get_pubkey_from_input(b'', witness)
    assert pk == bytes.fromhex(pubkey_hex)

def test_block_filter_creation():
    block_hash = "00"*32
    scripts = [b'\x00\x14' + b'\x11'*20, b'\x00\x14' + b'\x22'*20]
    
    f = create_block_filter(block_hash, scripts)
    assert isinstance(f, bytes)
    assert len(f) > 0
