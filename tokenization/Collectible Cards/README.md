# Documentación del Proyecto RWA-creations

Este proyecto es un sistema de **tokenización de cartas coleccionables** construido con contratos inteligentes en Solidity, pensado para ser vendido en marketplaces como OpenSea. Cada carta física o digital se divide en múltiples "fracciones" (tokens) siguiendo el estándar ERC-1155, permitiendo que varias personas sean dueñas de una parte de la misma carta.

## ¿Qué hace el sistema?

El contrato principal `CollectibleCard.sol` permite crear cartas digitales con nombre, descripción, cantidad de fracciones, precio por fracción y un porcentaje de regalías (royalties) para el creador. Usa contratos de **OpenZeppelin** (versión 5.6.1) para garantizar seguridad, incluyendo control de acceso (`AccessManaged`), pausado de emergencia (`Pausable`) y seguimiento de suministro (`Supply`).

### Funciones principales

- **createCard**: crea una nueva carta y mintea (genera) todas sus fracciones hacia una dirección.
- **modifyPrice / modifyRoyalty**: permite al administrador ajustar precio y regalías.
- **freezeMetadata**: congela los metadatos de una carta para que nadie pueda cambiarlos (importante para confianza en OpenSea).
- **royaltyInfo**: implementa el estándar EIP-2981 que OpenSea usa para pagar regalías automáticamente.
- **pause / unpause**: permite detener todas las transferencias en caso de emergencia.

## Requisitos previos

Necesitas tener instalado:

- **Git** (para clonar el repositorio)
- **Foundry** (herramienta que incluye `forge`, `cast` y `anvil`)
- Una **wallet** con fondos en la red donde se va a desplegar (ej. Sepolia, Base, Ethereum)
- Una **URL RPC** de un proveedor como Alchemy o Infura

## Instalación de Foundry

Foundry es el "taller" donde se compila y prueba el contrato. En Linux, macOS o WSL, abrir una terminal y ejecutar:

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

El primer comando descarga el instalador, y `foundryup` instala las últimas versiones de `forge`, `cast`, `anvil` y `chisel`.

## Clonar y preparar el proyecto

```bash
git clone <url-del-repo> RWA-creations
cd RWA-creations
forge install
```

El comando `forge install` descarga las dependencias listadas en `foundry.lock`: `forge-std v1.15.0`, `openzeppelin-contracts v5.6.1` y `openzeppelin-contracts-upgradeable v5.6.1`.

## Cómo ejecutarlo

### Compilar el contrato
```bash
forge build
```
Esto compila el archivo `src/CollectibleCard.sol` usando las rutas definidas en `remappings.txt`.

### Ejecutar las pruebas
```bash
forge test
```
Nota: actualmente el archivo `test/Counter.t.sol` está vacío, por lo que no hay pruebas reales todavía.

### Simulación local (opcional)
Para levantar una blockchain local, ejecutar:
```bash
anvil
```
Esto proporciona una red de pruebas gratuita en `http://127.0.0.1:8545` con cuentas precargadas.

## Cómo hacer el deploy

Como el contrato es **upgradeable** (actualizable) y hereda `Initializable`, no se usa un constructor normal, sino la función `initialize(address initialAuthority, address royalRetriever)`. El despliegue normalmente se hace detrás de un proxy.

### Pasos simplificados

1. **Configurar variables de entorno** en un archivo `.env`:
   ```bash
   PRIVATE_KEY=0xTuLlavePrivada
   RPC_URL=https://eth-sepolia.g.alchemy.com/v2/TU_API_KEY
   ETHERSCAN_API_KEY=TuClaveEtherscan
   ```

2. **Desplegar el contrato** con forge create:
   ```bash
   source .env
   forge create src/CollectibleCard.sol:CollectibleCard \
     --rpc-url $RPC_URL \
     --private-key $PRIVATE_KEY \
     --broadcast
   ```

3. **Inicializar el contrato** (porque el constructor está deshabilitado con `_disableInitializers()`):
   ```bash
   cast send <DIRECCION_DEL_CONTRATO> \
     "initialize(address,address)" <ADMIN_ADDRESS> <ROYALTY_RECEIVER> \
     --rpc-url $RPC_URL --private-key $PRIVATE_KEY
   ```

4. **Verificar el contrato en Etherscan** para que OpenSea lo reconozca correctamente:
   ```bash
   forge verify-contract <DIRECCION> src/CollectibleCard.sol:CollectibleCard \
     --chain sepolia --etherscan-api-key $ETHERSCAN_API_KEY
   ```

5. **Crear una carta** (ya en producción):
   ```bash
   cast send <DIRECCION> \
     "createCard(address,string,string,uint16,string,uint256,uint16)" \
     <destinatario> "Mi Carta" "Descripción" 100 "ipfs://.../meta.json" 1000000000000000 500 \
     --rpc-url $RPC_URL --private-key $PRIVATE_KEY
   ```
   El último número (500) representa 5% de regalías, ya que el contrato trabaja en base 10000 (100% = 10000).

## Publicación en OpenSea

Una vez desplegado y con cartas creadas, OpenSea detecta automáticamente los tokens ERC-1155 usando los metadatos del `tokenURI` (normalmente un enlace IPFS con JSON e imagen). El contrato ya implementa el estándar EIP-2981 (interfaz `0x2a55205a`), lo que significa que OpenSea reconocerá y pagará las regalías de forma automática.

## Estructura del repositorio

| Archivo | Propósito |
|---|---|
| `src/CollectibleCard.sol` | Contrato principal de tokenización |
| `script/Counter.s.sol` | Script de deploy (actualmente vacío) |
| `test/Counter.t.sol` | Archivo de pruebas (vacío) |
| `foundry.toml` | Configuración de Foundry |
| `remappings.txt` | Rutas a las librerías de OpenZeppelin |