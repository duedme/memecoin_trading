#!/usr/bin/env python3
"""
rpc_helpers.py
Funciones auxiliares para interactuar con el nodo RPC de Solana
"""

import json
import requests
import time
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class SolanaRPC:
    """Cliente RPC para Solana"""
    
    def __init__(self, rpc_url: str = "http://127.0.0.1:7211"):
        self.rpc_url = rpc_url
        self.request_count = 0
        
    def call(self, method: str, params: List = None) -> Dict:
        """Realiza una llamada JSON-RPC al nodo"""
        if params is None:
            params = []
            
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_count,
            "method": method,
            "params": params
        }
        
        self.request_count += 1
        
        try:
            response = requests.post(
                self.rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if "error" in result:
                logger.error(f"RPC Error: {result['error']}")
                return None
                
            return result.get("result")
            
        except Exception as e:
            logger.error(f"Error en RPC call {method}: {e}")
            return None
    
    def get_account_info(self, pubkey: str, encoding: str = "jsonParsed") -> Optional[Dict]:
        """Obtiene información de una cuenta"""
        return self.call("getAccountInfo", [pubkey, {"encoding": encoding}])
    
    def get_signatures_for_address(
        self, 
        address: str, 
        limit: int = 10,
        before: Optional[str] = None,
        until: Optional[str] = None
    ) -> List[Dict]:
        """Obtiene firmas de transacciones para una dirección"""
        params = [address, {"limit": limit}]
        
        if before:
            params[1]["before"] = before
        if until:
            params[1]["until"] = until
            
        result = self.call("getSignaturesForAddress", params)
        return result if result else []
    
    def get_transaction(
        self, 
        signature: str, 
        encoding: str = "jsonParsed",
        max_supported_version: int = 0
    ) -> Optional[Dict]:
        """Obtiene detalles de una transacción"""
        return self.call("getTransaction", [
            signature,
            {
                "encoding": encoding,
                "maxSupportedTransactionVersion": max_supported_version
            }
        ])
    
    def get_token_accounts_by_owner(
        self,
        owner: str,
        mint: Optional[str] = None,
        program_id: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    ) -> List[Dict]:
        """Obtiene cuentas de tokens de un propietario"""
        params = [owner, {"programId": program_id}]
        
        if mint:
            params[1] = {"mint": mint}
            
        result = self.call("getTokenAccountsByOwner", params)
        if result and "value" in result:
            return result["value"]
        return []
    
    def get_program_accounts(
        self,
        program_id: str,
        filters: List[Dict] = None,
        data_slice: Dict = None,
        encoding: str = "jsonParsed"
    ) -> List[Dict]:
        """Obtiene cuentas de un programa con filtros"""
        config = {"encoding": encoding}
        
        if filters:
            config["filters"] = filters
        if data_slice:
            config["dataSlice"] = data_slice
            
        result = self.call("getProgramAccounts", [program_id, config])
        return result if result else []
    
    def get_multiple_accounts(
        self,
        pubkeys: List[str],
        encoding: str = "jsonParsed"
    ) -> List[Dict]:
        """Obtiene información de múltiples cuentas en una sola llamada"""
        result = self.call("getMultipleAccountsInfo", [
            pubkeys,
            {"encoding": encoding}
        ])
        
        if result and "value" in result:
            return result["value"]
        return []


def decode_pool_data(account_data: Dict, amm_type: str) -> Optional[Dict]:
    """
    Decodifica los datos de un pool según el tipo de AMM
    Retorna: {
        'reserves_base': int,
        'reserves_quote': int,
        'base_mint': str,
        'quote_mint': str,
        'decimals_base': int,
        'decimals_quote': int
    }
    """
    try:
        if not account_data or "data" not in account_data:
            return None
        
        data = account_data["data"]
        
        # Para pools parseados (jsonParsed)
        if isinstance(data, dict) and "parsed" in data:
            parsed = data["parsed"]["info"]
            
            # Estructura común en muchos AMMs
            return {
                "reserves_base": int(parsed.get("tokenAmount", {}).get("amount", 0)),
                "reserves_quote": int(parsed.get("tokenAmount2", {}).get("amount", 0)),
                "base_mint": parsed.get("mint", ""),
                "quote_mint": parsed.get("mint2", ""),
                "decimals_base": parsed.get("tokenAmount", {}).get("decimals", 9),
                "decimals_quote": parsed.get("tokenAmount2", {}).get("decimals", 9)
            }
        
        # Para datos raw (base64/base58)
        # Aquí necesitarías parsers específicos por AMM
        # Por ahora retornamos None
        logger.warning(f"Pool data en formato raw para {amm_type}, parser no implementado")
        return None
        
    except Exception as e:
        logger.error(f"Error decodificando pool data: {e}")
        return None


def calculate_price_from_reserves(
    reserves_base: int,
    reserves_quote: int,
    decimals_base: int = 9,
    decimals_quote: int = 9,
    invert: bool = False
) -> float:
    """
    Calcula el precio de un token desde las reserves del pool
    
    Args:
        reserves_base: Reserves del token base
        reserves_quote: Reserves del token quote (usualmente SOL/USDC)
        decimals_base: Decimales del token base
        decimals_quote: Decimales del token quote
        invert: Si True, invierte el precio (quote/base en lugar de base/quote)
    
    Returns:
        Precio del token
    """
    try:
        if reserves_base == 0 or reserves_quote == 0:
            return 0.0
        
        # Ajustar por decimales
        base_adjusted = reserves_base / (10 ** decimals_base)
        quote_adjusted = reserves_quote / (10 ** decimals_quote)
        
        if invert:
            return quote_adjusted / base_adjusted
        else:
            return base_adjusted / quote_adjusted
            
    except Exception as e:
        logger.error(f"Error calculando precio: {e}")
        return 0.0


def count_token_holders(rpc: SolanaRPC, mint_address: str) -> int:
    """
    Cuenta el número de holders de un token
    
    Args:
        rpc: Cliente RPC
        mint_address: Dirección del token mint
    
    Returns:
        Número de holders
    """
    try:
        # Filtrar por mint address
        filters = [
            {"dataSize": 165},  # Tamaño de token account
            {
                "memcmp": {
                    "offset": 0,
                    "bytes": mint_address
                }
            }
        ]
        
        # Obtener cuentas (solo necesitamos el conteo, no los datos)
        accounts = rpc.get_program_accounts(
            program_id="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            filters=filters,
            data_slice={"offset": 0, "length": 0}  # No necesitamos datos
        )
        
        return len(accounts)
        
    except Exception as e:
        logger.error(f"Error contando holders para {mint_address}: {e}")
        return 0


def parse_swap_transaction(tx: Dict) -> Optional[Dict]:
    """
    Parsea una transacción de swap y extrae información relevante
    
    Returns:
        {
            'signature': str,
            'block_time': int,
            'wallet': str,
            'token_in': str,
            'token_out': str,
            'amount_in': float,
            'amount_out': float,
            'type': 'buy' | 'sell',
            'program_id': str,
            'success': bool
        }
    """
    try:
        if not tx or "meta" not in tx:
            return None
        
        meta = tx["meta"]
        
        # Verificar que la transacción fue exitosa
        if meta.get("err") is not None:
            return None
        
        # Extraer información básica
        signature = tx.get("transaction", {}).get("signatures", [None])[0]
        block_time = tx.get("blockTime")
        
        # Analizar token balances pre y post
        pre_balances = meta.get("preTokenBalances", [])
        post_balances = meta.get("postTokenBalances", [])
        
        if not pre_balances or not post_balances:
            return None
        
        # Encontrar cambios en balances
        changes = []
        for pre in pre_balances:
            account = pre.get("accountIndex")
            mint = pre.get("mint")
            pre_amount = int(pre.get("uiTokenAmount", {}).get("amount", 0))
            
            # Buscar el balance post correspondiente
            post = next((p for p in post_balances if p.get("accountIndex") == account), None)
            if post:
                post_amount = int(post.get("uiTokenAmount", {}).get("amount", 0))
                diff = post_amount - pre_amount
                
                if diff != 0:
                    changes.append({
                        "mint": mint,
                        "diff": diff,
                        "decimals": pre.get("uiTokenAmount", {}).get("decimals", 9)
                    })
        
        # Si hay exactamente 2 cambios (entrada y salida), es un swap
        if len(changes) == 2:
            # El negativo es la entrada (lo que se pagó)
            # El positivo es la salida (lo que se recibió)
            token_in = next((c for c in changes if c["diff"] < 0), None)
            token_out = next((c for c in changes if c["diff"] > 0), None)
            
            if token_in and token_out:
                # Determinar wallet (fee payer)
                wallet = tx.get("transaction", {}).get("message", {}).get("accountKeys", [{}])[0].get("pubkey", "")
                
                # Determinar programa usado
                program_id = ""
                instructions = tx.get("transaction", {}).get("message", {}).get("instructions", [])
                if instructions:
                    program_id = instructions[0].get("programId", {}).get("pubkey", "")
                
                return {
                    "signature": signature,
                    "block_time": block_time,
                    "wallet": wallet,
                    "token_in": token_in["mint"],
                    "token_out": token_out["mint"],
                    "amount_in": abs(token_in["diff"]) / (10 ** token_in["decimals"]),
                    "amount_out": token_out["diff"] / (10 ** token_out["decimals"]),
                    "type": "buy" if token_out["diff"] > 0 else "sell",
                    "program_id": program_id,
                    "success": True
                }
        
        return None
        
    except Exception as e:
        logger.error(f"Error parseando swap transaction: {e}")
        return None


def batch_process_transactions(
    rpc: SolanaRPC,
    signatures: List[str],
    max_workers: int = 5
) -> List[Dict]:
    """
    Procesa múltiples transacciones en paralelo
    
    Args:
        rpc: Cliente RPC
        signatures: Lista de firmas a procesar
        max_workers: Número de workers concurrentes
    
    Returns:
        Lista de transacciones parseadas
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    results = []
    
    def fetch_and_parse(sig):
        tx = rpc.get_transaction(sig)
        if tx:
            return parse_swap_transaction(tx)
        return None
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_and_parse, sig): sig for sig in signatures}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error procesando transacción: {e}")
    
    return results


def rate_limit_sleep(calls_made: int, max_calls_per_second: int = 50):
    """
    Implementa rate limiting simple
    
    Args:
        calls_made: Número de llamadas realizadas en el periodo actual
        max_calls_per_second: Límite de llamadas por segundo
    """
    if calls_made >= max_calls_per_second:
        time.sleep(1)
        return 0
    return calls_made
