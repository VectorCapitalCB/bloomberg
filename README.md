# ORB-BLOOMBERG

Redistribuidor (ORB / fan-out) de market data de **Bloomberg** sobre el protocolo Netty+protobuf
de VectorCapital. Es el gemelo del `ORB-BCS` pero, en vez de ingerir por **FIX**, se suscribe a una
**Terminal Bloomberg** por el **Desktop API (DAPI)**, servicio `//blp/mktdata` (top-of-book), y
re-publica los ticks a los clientes que se suscriben al ORB.

```
Bloomberg Terminal                ORB-BLOOMBERG (.exe / servicio Windows)
  localhost:8194  ──BLPAPI──>  BloombergSession ──BbgTick──> InversorBloombergToProto
  //blp/mktdata                (1 sub por security)          (tick -> Statistic/DataBook/Trade)
                                                                     │ eventBus
                                                                     ▼
                                              TopicDistributor (fan-out) ──> ClientManager
                                                                              NettyProtobufServer :8050
                                                                                   │
                                                                          Clientes suscritos (A, B, C...)
```

## Arquitectura (qué cambió vs ORB-BCS)

| Capa | ORB-BCS (FIX) | ORB-BLOOMBERG |
|---|---|---|
| Ingesta | `QuickFixApp` + `InversorFixToProto` | `bbg/BloombergSession` (BLPAPI) + `ss/InversorBloombergToProto` |
| Suscripción | `ExchangeSubscriptionRegistry` (35=V) | dedup por security dentro de `BloombergSession` (`CorrelationID`) |
| Fan-out / Netty / Kafka / proto | — | **idéntico** (`TopicDistributor`, `ClientManager`, `NettyProtobufServer`) |
| `securityExchange` | `BCS` | `BLOOMBERG_MKD` (enum ya existente en `marketdata.proto`) |

La ingesta BLPAPI (`src/bloomberg/java`) está **aislada bajo el perfil Maven `-Pbloomberg`** y se
carga por reflexión, de modo que el build por defecto compila y corre **sin** `blpapi` (modo simulador).

## Build

```bat
REM Por defecto (simulador): NO necesita blpapi
mvn -DskipTests package
REM -> target\ORB-BLOOMBERG-1.0-fat.jar
```

## Correr en SIMULADOR (sin Terminal ni blpapi)

```bat
run-simulator.bat
REM = java -jar target\ORB-BLOOMBERG-1.0-fat.jar config\SIMULATOR.properties
```
Levanta el fan-out en `0.0.0.0:8050` y genera data sintética para los instrumentos de
`config\simulator-prices.json`.

## Correr contra BLOOMBERG en vivo

Requiere el **SDK de Bloomberg** (no está en Maven Central) y la Terminal corriendo:

```bat
REM 1) Instala el blpapi.jar (del SDK) en tu .m2
install-blpapi.bat C:\ruta\blpapi-3.24.6-1.jar 3.24.6-1

REM 2) Compila la ingesta Bloomberg
mvn -Pbloomberg -DskipTests package

REM 3) Arranca (ajusta BLPAPI_DLL_DIR a donde está blpapi3_64.dll, p.ej. C:\blp\DAPI)
run-bloomberg.bat
```

Config en `config\BLOOMBERG.properties` (host/puerto DAPI, campos a suscribir, sufijo de ticker).
Los clientes se suscriben con el ticker Bloomberg como `symbol` (p.ej. `IBM US Equity`, `EUR Curncy`),
o con `bloomberg.security.suffix` para completarlo.

## Pendiente

- **Empaquetado como `.exe` / servicio Windows** (jpackage o WinSW) — etapa 4.
