from eth_utils import decode_hex
from eth_hash.auto import keccak
from web3 import Web3

from openfabric_pysdk.logger import logger

def recover_wallet_address(message: str, signature: str) -> str:

    web3 = Web3(Web3.HTTPProvider('https://rpc.openfabric.network'))
    logger.debug(f"Message: {message} Signature: {signature}")
    message_hash = keccak(message.encode('utf-8'))
    recovered_address = str(web3.eth.account._recover_hash(message_hash, None, decode_hex(signature)))

    if recovered_address.startswith('0x'):
        return recovered_address[2:]

    logger.debug(f"Recovered address: {recovered_address}")
    return recovered_address

def check_matching_challenge(refChallenge: dict, challenge2: dict) -> bool:

    for key in refChallenge.keys():
        if key not in challenge2:
            return False
        
        if key == 'signature':
            continue
        
        if refChallenge[key] != challenge2[key]:
            return False
    
    return True