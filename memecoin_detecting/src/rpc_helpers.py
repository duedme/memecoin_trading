#!/usr/bin/env python3
"""
rpc_helpers.py - Funciones auxiliares para interactuar con el nodo RPC de Solana
Versión corregida con asyncio para paralelización
"""

import json
import asyncio
import aiohttp
import requests
import time
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class SolanaRPC:
    """Cliente RPC para Solana (versión síncrona)"""
    
    def __init__(self, rpc_url: str = "http://127.0.0.1:7211"):
        self.rpc_url = rpc_url
        self.request_count = 0
    
    def call(self, method: str, params: List = None) -> Optional[Any]:
        """
        Realiza una llamada JSON-RPC al nodo
        
        IMPORTANTE: Retorna DIRECTAMENTE el contenido de result["result"]
        Si hay error, retorna None
        
        Args:
            method: Método RPC (ej: "getAccountInfo")
            params: Lista de parámetros
            
        Returns:
            El contenido de result["result"] o None si hay error
        """
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
            
            # Retornamos directamente el contenido de "result"
            return result.get("result")
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout en RPC call: {method}")
            return None
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
        """
        Obtiene información de múltiples cuentas en una sola llamada
        CORREGIDO: Era getMultipleAccountsInfo (incorrecto) → getMultipleAccounts
        """
        result = self.call("getMultipleAccounts", [pubkeys, {"encoding": encoding}])
        if result and "value" in result:
            return result["value"]
        return []


class AsyncSolanaRPC:
    """Cliente RPC asíncrono para Solana - permite múltiples llamadas en paralelo"""
    
    def __init__(self, rpc_url: str = "http://127.0.0.1:7211", max_concurrent: int = 20):
        self.rpc_url = rpc_url
        self.request_count = 0
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
    
    async def __aenter__(self):
        """Context manager para manejar sesión de aiohttp"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cerrar sesión al salir del context manager"""
        if self.session:
            await self.session.close()
    
    async def call(self, method: str, params: List = None) -> Optional[Any]:
        """
        Llamada RPC asíncrona
        
        IMPORTANTE: Retorna DIRECTAMENTE el contenido de result["result"]
        Si hay error, retorna None
        """
        if params is None:
            params = []
        
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_count,
            "method": method,
            "params": params
        }
        self.request_count += 1
        
        async with self.semaphore:  # Limita concurrencia
            try:
                async with self.session.post(
                    self.rpc_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    result = await response.json()
                    
                    if "error" in result:
                        logger.error(f"RPC Error: {result['error']}")
                        return None
                    
                    return result.get("result")
                    
            except asyncio.TimeoutError:
                logger.error(f"Timeout en RPC call: {method}")
                return None
            except Exception as e:
                logger.error(f"Error en RPC call {method}: {e}")
                return None
    
    async def get_account_info(self, pubkey: str, encoding: str = "jsonParsed") -> Optional[Dict]:
        """Obtiene información de una cuenta (async)"""
        return await self.call("getAccountInfo", [pubkey, {"encoding": encoding}])
    
    async def get_token_largest_accounts(self, mint: str) -> Optional[Dict]:
        """Obtiene las cuentas con más tokens (async)"""
        return await self.call("getTokenLargestAccounts", [mint])
    
    async def get_multiple_accounts(
        self, 
        pubkeys: List[str], 
        encoding: str = "jsonParsed"
    ) -> List[Dict]:
        """Obtiene información de múltiples cuentas (async)"""
        result = await self.call("getMultipleAccounts", [pubkeys, {"encoding": encoding}])
        if result and "value" in result:
            return result["value"]
        return []


# ============================================
# FUNCIONES AUXILIARES PARA PARSEO
# ============================================

def parse_swap_transaction(tx: Dict) -> Optional[Dict]:
    """
    Parsea una transacción de swap. Soporta:
    1. Token-to-Token swaps (Orca, Raydium, etc.)
    2. SOL-to-Token swaps (Pump.fun, PumpSwap, etc.)
    """
    try:
        if not tx or "meta" not in tx:
            return None

        meta = tx["meta"]
        if meta.get("err"):
            return None

        pre_token = meta.get("preTokenBalances", [])
        post_token = meta.get("postTokenBalances", [])

        if not pre_token and not post_token:
            return None

        WSOL = "So11111111111111111111111111111111111111112"

        # === Track per (accountIndex, mint) para no mezclar cuentas del mismo token ===
        account_changes = {}
        for b in pre_token:
            key = (b["accountIndex"], b["mint"])
            account_changes[key] = {
                "mint": b["mint"],
                "pre": int(b["uiTokenAmount"]["amount"]),
                "post": 0,
                "decimals": b["uiTokenAmount"].get("decimals", 9),
            }

        for b in post_token:
            key = (b["accountIndex"], b["mint"])
            if key in account_changes:
                account_changes[key]["post"] = int(b["uiTokenAmount"]["amount"])
            else:
                account_changes[key] = {
                    "mint": b["mint"],
                    "pre": 0,
                    "post": int(b["uiTokenAmount"]["amount"]),
                    "decimals": b["uiTokenAmount"].get("decimals", 9),
                }

        # Separar cuentas que aumentaron vs disminuyeron
        token_increases = []
        token_decreases = []

        for key, data in account_changes.items():
            diff = data["post"] - data["pre"]
            if diff > 0:
                token_increases.append((data["mint"], diff, data["decimals"]))
            elif diff < 0:
                token_decreases.append((data["mint"], abs(diff), data["decimals"]))

        # SOL nativo change en signer (account[0])
        pre_sol = meta.get("preBalances", [])
        post_sol = meta.get("postBalances", [])
        sol_change_lamports = 0
        if pre_sol and post_sol:
            sol_change_lamports = post_sol[0] - pre_sol[0]

        # === Determinar swap ===
        token_in_mint = None
        token_out_mint = None
        amount_in = 0.0
        amount_out = 0.0

        if token_increases and token_decreases:
            inc_mints = set(m for m, _, _ in token_increases)
            dec_mints = set(m for m, _, _ in token_decreases)
            diff_inc = inc_mints - dec_mints
            diff_dec = dec_mints - inc_mints

            if diff_inc and diff_dec:
                # Caso 1: Dos tokens diferentes → swap estándar (Raydium, Orca, etc.)
                for mint, amt, dec in token_increases:
                    if mint in diff_inc:
                        token_in_mint = mint
                        amount_in = amt / (10 ** dec)
                        break
                for mint, amt, dec in token_decreases:
                    if mint in diff_dec:
                        token_out_mint = mint
                        amount_out = amt / (10 ** dec)
                        break

            elif abs(sol_change_lamports) > 1_000_000:
                # Caso 2: Mismo token se mueve entre cuentas + SOL cambia → Pump.fun
                involved = list(inc_mints | dec_mints)
                if involved:
                    token_mint = involved[0]
                    token_decimals = 9
                    token_amount = 0
                    for mint, amt, dec in token_decreases:
                        if mint == token_mint:
                            token_amount = amt / (10 ** dec)
                            token_decimals = dec
                            break

                    sol_amount = abs(sol_change_lamports) / 1_000_000_000

                    if sol_change_lamports < 0:  # BUY: wallet gastó SOL, recibió tokens
                        token_in_mint = token_mint    # wallet RECIBIÓ tokens
                        token_out_mint = WSOL         # wallet DIÓ SOL
                        amount_in = token_amount      # cantidad recibida
                        amount_out = sol_amount       # cantidad dada
                    else:  # SELL: wallet recibió SOL, dio tokens
                        token_in_mint = WSOL          # wallet RECIBIÓ SOL
                        token_out_mint = token_mint   # wallet DIÓ tokens
                        amount_in = sol_amount        # cantidad recibida
                        amount_out = token_amount     # cantidad dada

        elif abs(sol_change_lamports) > 1_000_000:
            # Caso 3: Solo 1 lado tiene token changes + SOL cambió
            sol_amount = abs(sol_change_lamports) / 1_000_000_000

            if token_increases and not token_decreases:
                # Tokens aparecieron + SOL disminuyó → BUY
                mint, amt, dec = token_increases[0]
                token_in_mint = mint
                token_out_mint = WSOL
                amount_in = amt / (10 ** dec)
                amount_out = sol_amount
            elif token_decreases and not token_increases:
                # Tokens desaparecieron + SOL aumentó → SELL
                mint, amt, dec = token_decreases[0]
                token_in_mint = WSOL
                token_out_mint = mint
                amount_in = sol_amount
                amount_out = amt / (10 ** dec)

        if not token_in_mint or not token_out_mint:
            return None

        # Extraer wallet (primer signer)
        wallet = None
        if "transaction" in tx and "message" in tx["transaction"]:
            account_keys = tx["transaction"]["message"].get("accountKeys", [])
            if account_keys:
                wallet = account_keys[0] if isinstance(account_keys[0], str) else account_keys[0].get("pubkey")

        signature = tx.get("transaction", {}).get("signatures", [None])[0]
        blocktime = tx.get("blockTime")

        # Program ID (saltar system/compute/token programs)
        SKIP_PROGRAMS = {
            "11111111111111111111111111111111",
            "ComputeBudget111111111111111111111111111111",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        }
        program_id = None
        for ix in tx.get("transaction", {}).get("message", {}).get("instructions", []):
            pid = ix.get("programId")
            if isinstance(pid, dict):
                pid = pid.get("pubkey")
            if pid and pid not in SKIP_PROGRAMS:
                program_id = pid
                break

        # Determinar tipo desde perspectiva del wallet
        if token_in_mint == WSOL:
            tx_type = 'sell'
        elif token_out_mint == WSOL:
            tx_type = 'buy'
        else:
            tx_type = 'token_swap'  # Ni token_in ni token_out es WSOL

        return {
            "signature": signature,
            "blocktime": blocktime,
            "wallet": wallet,
            "token_in": token_in_mint,
            "token_out": token_out_mint,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "type": tx_type,
            "program_id": program_id,
            "success": True
        }

    except Exception as e:
        logger.error(f"Error parseando swap transaction: {e}")
        return None

def batch_process_transactions(transactions: List[Dict]) -> List[Dict]:
    """
    Procesa un lote de transacciones y extrae swaps
    
    Args:
        transactions: Lista de transacciones crudas
        
    Returns:
        Lista de swaps parseados
    """
    swaps = []
    for tx in transactions:
        swap = parse_swap_transaction(tx)
        if swap:
            swaps.append(swap)
    return swaps


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
        # Filtros para getProgramAccounts
        filters = [
            {
                "dataSize": 165  # Tamaño de TokenAccount
            },
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


def calculate_price_from_pool(
    reserves_base: int,
    reserves_quote: int, 
    decimals_base: int,
    decimals_quote: int,
    invert: bool = False
) -> float:
    """
    Calcula precio de un pool AMM usando x*y=k
    
    Args:
        reserves_base: Reservas del token base (raw)
        reserves_quote: Reservas del token quote (raw)
        decimals_base: Decimales del token base
        decimals_quote: Decimales del token quote
        invert: Si True, retorna quote/base en lugar de base/quote
        
    Returns:
        Precio calculado
    """
    try:
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
