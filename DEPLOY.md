# Despliegue de ORB-BLOOMBERG — paso a paso

## Topología

| Máquina | Rol | Necesita |
|---|---|---|
| **PC-DEV** | compilar y generar el `.msi` | JDK 21, WiX v3.11, SDK Bloomberg (jar+dll) |
| **SERVIDOR** | guardar/distribuir el `.msi` (recurso compartido `\\SERVIDOR\orb\`) | una carpeta compartida |
| **PC-BLOOMBERG** | correr el servicio (producción) | la Terminal Bloomberg logueada |

> PC-BLOOMBERG **no** necesita Java ni el SDK: el `.msi` trae el JRE, el blpapi.jar y el dll embebidos.

---

## A. Preparar PC-DEV (una sola vez)

1. Instalar **JDK 21** (ya está) y **WiX Toolset v3.11** → https://wixtoolset.org
2. Del **SDK de Bloomberg** sacar dos archivos: `blpapi-3.24.6-1.jar` y `blpapi3_64.dll`.
3. Registrar el jar en Maven:
   ```bat
   install-blpapi.bat C:\ruta\blpapi-3.24.6-1.jar 3.24.6-1
   ```
4. Dejar el dll en la carpeta `lib\` del proyecto:
   ```bat
   mkdir lib
   copy C:\ruta\blpapi3_64.dll lib\
   ```
   (el `build-installer.bat` lo toma de ahí automáticamente)

---

## B. Generar el instalador (en PC-DEV, cada release)

```bat
cd E:\VC-GITHUB\bloomberg
build-installer.bat
```
Resultado: `build\dist\ORB-BLOOMBERG-1.0.msi`

---

## C. Publicar al SERVIDOR

```bat
copy build\dist\ORB-BLOOMBERG-1.0.msi \\SERVIDOR\orb\
```
(Recomendado: guardar cada versión, p.ej. `\\SERVIDOR\orb\ORB-BLOOMBERG-1.0.msi`, `...-1.1.msi`, etc.)

---

## D. Instalar en PC-BLOOMBERG (primera vez)

1. Asegurarse de que la **Terminal Bloomberg esté abierta y logueada**.
2. Abrir un **CMD como Administrador** y correr:
   ```bat
   msiexec /i \\SERVIDOR\orb\ORB-BLOOMBERG-1.0.msi /qn
   ```
   (o doble clic al `.msi` desde la carpeta del servidor)
3. Esto instala en `C:\Program Files\ORB-BLOOMBERG\`, registra el **servicio `ORB-BLOOMBERG`** y lo arranca.
4. La primera vez se crea la config en `C:\ProgramData\ORB-BLOOMBERG\config\BLOOMBERG.properties`.
   Revisarla (host/puerto DAPI, campos). El default `localhost:8194` normalmente sirve.

---

## E. Verificar que funciona

1. **Servicio corriendo:**
   ```bat
   sc query ORB-BLOOMBERG
   ```
   (debe decir `RUNNING`)
2. **Log de conexión y data:**
   ```bat
   type C:\ProgramData\ORB-BLOOMBERG\logs\orb-bloomberg.log
   ```
   Buscar:
   - `BLOOMBERG: CONECTADO host=localhost:8194` → conectó a la Terminal
   - `SUSCRITO security='...'` → suscripción enviada
   - `DATA OK ... (primer tick recibido)` y `ticksRecibidos=...` → **está llegando data**
3. Tu cliente se conecta al fan-out en `PC-BLOOMBERG:8050` y manda `Subscribe` con el ticker Bloomberg (`IBM US Equity`).

---

## F. Actualizar a una versión nueva

1. En **PC-DEV**: subir la versión en `build-installer.bat`:
   ```bat
   set APP_VERSION=1.1
   ```
2. Reconstruir y publicar:
   ```bat
   build-installer.bat
   copy build\dist\ORB-BLOOMBERG-1.1.msi \\SERVIDOR\orb\
   ```
3. En **PC-BLOOMBERG** (CMD como Administrador):
   ```bat
   msiexec /i \\SERVIDOR\orb\ORB-BLOOMBERG-1.1.msi /qn
   ```
   Windows detecta que es una actualización (mismo `UpgradeCode`, versión mayor) → **detiene el servicio, reemplaza archivos y lo reinicia**.
   **Tu config y tus logs en `C:\ProgramData\ORB-BLOOMBERG\` se conservan.**

> Regla de oro: **nunca cambiar el `UPGRADE_UUID`** del `build-installer.bat` y **siempre subir `APP_VERSION`**. Eso es lo que hace que actualice en sitio.

---

## G. Problemas comunes

- **El servicio corre pero NO conecta a Bloomberg** (lo más probable de ver):
  El servicio corre como `LocalSystem` en la "Sesión 0", separada de la sesión donde está la Terminal.
  Solución: `services.msc` → **ORB-BLOOMBERG** → *Propiedades* → pestaña **Iniciar sesión** →
  **Esta cuenta** → poner el **usuario de Windows que abre la Terminal** + contraseña → reiniciar el servicio.
- **`SubscriptionFailure` en el log**: el ticker no existe / sin entitlement. Revisar que el cliente mande el ticker Bloomberg completo (`IBM US Equity`, `EUR Curncy`).
- **No hay data y la Terminal está cerrada**: la Terminal debe estar **abierta y logueada** en PC-BLOOMBERG (el DAPI vive ahí).
- **Comandos útiles del servicio:**
  ```bat
  sc stop ORB-BLOOMBERG
  sc start ORB-BLOOMBERG
  sc query ORB-BLOOMBERG
  ```

---

## H. (Opcional) Auto-update desde el servidor

Si quieres que PC-BLOOMBERG se actualice **solo** desde `\\SERVIDOR\orb\`:
se agrega una **tarea programada** que compara la versión instalada contra un `latest.txt`
en el servidor y, si hay una nueva, corre el `.msi` en silencio. (Pendiente de implementar — pedir si se desea.)
