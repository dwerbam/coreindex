import asyncio
import sys
import hashlib
from src.rpc import BitcoinRPC

async def main():
    if len(sys.argv) < 2:
        print("Usage: python addr_to_hash.py <bitcoin_address>")
        sys.exit(1)

    address = sys.argv[1]
    rpc = BitcoinRPC()
    
    try:
        # Use validateaddress to get scriptPubKey
        info = await rpc.call("validateaddress", [address])
        
        if not info["isvalid"]:
            print(f"Error: Invalid address '{address}'")
            return

        # Some versions of Core return scriptPubKey directly, others in embedded fields
        # validateaddress usually returns 'scriptPubKey' (hex)
        script_hex = info.get("scriptPubKey")
        
        # If not present (e.g. newer Core versions might need getaddressinfo for some details, 
        # but validateaddress usually gives the hex for simple validation)
        # Let's try getaddressinfo if scriptPubKey is missing or short
        if not script_hex:
             # Try getaddressinfo which provides more details for wallet addresses, 
             # but works for any if we just need scriptPubKey construction? 
             # Actually validateaddress in recent Core DOES return scriptPubKey
             pass

        print(f"Address: {address}")
        print(f"Script (Hex): {script_hex}")
        
        # Calculate Electrum ScriptHash: sha256(script).reverse()
        script_bytes = bytes.fromhex(script_hex)
        sh = hashlib.sha256(script_bytes).digest()[::-1].hex()
        
        print(f"Script Hash: {sh}")
        print(f"\nCheck this hash with: uv run query.py {sh}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await rpc.close()

if __name__ == "__main__":
    asyncio.run(main())
