# ---------------------------------------------------------------------------
# Reguera, Luciano Oscar
# Comisión D
# 30-10-2026
# ---------------------------------------------------------------------------

"""
Sistema Distribuido de Gestión de Tareas — Servidor TCP con Pool de Workers.

Recibe tareas y operaciones de múltiples clientes mediante sockets TCP,
las encola en una cola de mensajes (queue.Queue) y las distribuye entre
un pool de hilos workers para su procesamiento concurrente y persistencia
en SQLite.

Acciones soportadas (protocolo JSON por línea sobre TCP):
    registro           Registra un usuario nuevo.
    login              Verifica credenciales de un usuario.
    listar_tareas      Lista las tareas del usuario autenticado.
    crear_tarea        Crea una tarea nueva (opcionalmente simula carga).
    actualizar_tarea   Modifica título, descripción o estado de una tarea.
    eliminar_tarea     Elimina una tarea del usuario autenticado.

Ejecución:
    python servidor.py [puerto] [num_workers]
"""

from concurrent.futures import Future
import hashlib
import hmac
import itertools
import json
import queue
import secrets
import socket
import sqlite3
import sys
import threading
import time

try:
    from werkzeug.security import check_password_hash, generate_password_hash
except ImportError:
    # Respaldo compatible con el formato scrypt de Werkzeug usando la biblioteca estándar
    def generate_password_hash(password: str) -> str:
        salt = secrets.token_hex(8)
        digest = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt.encode("utf-8"),
            n=32768,
            r=8,
            p=1,
            maxmem=67108864,
            dklen=64,
        ).hex()
        return f"scrypt:32768:8:1${salt}${digest}"

    def check_password_hash(pwhash: str, password: str) -> bool:
        try:
            metodo, salt, digest_guardado = pwhash.split("$", 2)
            _, n_str, r_str, p_str = metodo.split(":")
            digest_calculado = hashlib.scrypt(
                password.encode("utf-8"),
                salt=salt.encode("utf-8"),
                n=int(n_str),
                r=int(r_str),
                p=int(p_str),
                maxmem=67108864,
                dklen=64,
            ).hex()
            return hmac.compare_digest(digest_guardado, digest_calculado)
        except ValueError:
            return False


# ---------------------------------------------------------------------------
# Configuración general
# ---------------------------------------------------------------------------

HOST = "localhost"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
NUM_WORKERS = int(sys.argv[2]) if len(sys.argv) > 2 else 4
DB_PATH = "tareas.db"

# Cola de mensajes interna para distribuir las tareas entre los workers
cola_tareas: queue.Queue = queue.Queue()

# Lock para sincronizar escrituras en SQLite entre múltiples hilos workers
db_lock = threading.Lock()

# Contador atómico para numerar cada tarea que ingresa al servidor
contador_tareas = itertools.count(1)


# ---------------------------------------------------------------------------
# Base de datos (SQLite thread-safe)
# ---------------------------------------------------------------------------

def conectar_db() -> sqlite3.Connection:
    """Abre una conexión SQLite configurada para el hilo actual."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    """Crea las tablas de usuarios y tareas si todavía no existen."""
    try:
        with db_lock, conectar_db() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS usuarios (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario       TEXT    NOT NULL UNIQUE,
                    password_hash TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tareas (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario_id  INTEGER NOT NULL,
                    titulo      TEXT    NOT NULL,
                    descripcion TEXT    NOT NULL DEFAULT '',
                    completada  INTEGER NOT NULL DEFAULT 0,
                    creada_en   TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
                );
                """
            )
        print(f"[DB] Base de datos '{DB_PATH}' lista.")
    except sqlite3.Error as error:
        print(f"[DB] Error al inicializar la base de datos: {error}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Utilidades y autenticación
# ---------------------------------------------------------------------------

def extraer_credenciales(datos: dict):
    """Extrae usuario y contraseña de un diccionario. Devuelve (usuario, contraseña) o None."""
    if not isinstance(datos, dict):
        return None
    usuario = str(datos.get("usuario", "")).strip()
    contrasena = str(datos.get("contraseña", ""))
    if not usuario or not contrasena:
        return None
    return usuario, contrasena


def verificar_usuario(conn: sqlite3.Connection, usuario: str, contrasena: str):
    """Devuelve la fila del usuario si las credenciales son válidas, o None."""
    fila = conn.execute(
        "SELECT id, usuario, password_hash FROM usuarios WHERE usuario = ?",
        (usuario,),
    ).fetchone()
    if fila and check_password_hash(fila["password_hash"], contrasena):
        return fila
    return None


def autenticar_peticion(conn: sqlite3.Connection, peticion: dict):
    """Valida el bloque 'auth' de la petición. Devuelve la fila del usuario o None."""
    credenciales = extraer_credenciales(peticion.get("auth") or {})
    if credenciales is None:
        return None
    return verificar_usuario(conn, *credenciales)


def tarea_a_dict(fila: sqlite3.Row) -> dict:
    return {
        "id": fila["id"],
        "titulo": fila["titulo"],
        "descripcion": fila["descripcion"],
        "completada": bool(fila["completada"]),
        "creada_en": fila["creada_en"],
    }


def obtener_tarea(conn: sqlite3.Connection, tarea_id: int, usuario_id: int):
    """Busca una tarea perteneciente al usuario autenticado."""
    return conn.execute(
        "SELECT * FROM tareas WHERE id = ? AND usuario_id = ?",
        (tarea_id, usuario_id),
    ).fetchone()


def respuesta_exito(codigo: int, datos: dict, worker: str) -> dict:
    return {"ok": True, "codigo": codigo, "worker": worker, "datos": datos}


def respuesta_error(codigo: int, mensaje: str, worker: str) -> dict:
    return {"ok": False, "codigo": codigo, "worker": worker, "datos": {"error": mensaje}}


# ---------------------------------------------------------------------------
# Lógica de negocio ejecutada por los Workers
# ---------------------------------------------------------------------------

def accion_registro(conn: sqlite3.Connection, datos: dict, worker: str) -> dict:
    credenciales = extraer_credenciales(datos)
    if credenciales is None:
        return respuesta_error(400, "Se requieren 'usuario' y 'contraseña'", worker)
    usuario, contrasena = credenciales
    password_hash = generate_password_hash(contrasena)

    try:
        with db_lock:
            conn.execute(
                "INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)",
                (usuario, password_hash),
            )
            conn.commit()
    except sqlite3.IntegrityError:
        return respuesta_error(409, f"El usuario '{usuario}' ya existe", worker)

    return respuesta_exito(
        201,
        {"mensaje": f"Usuario '{usuario}' registrado correctamente"},
        worker,
    )


def accion_login(conn: sqlite3.Connection, datos: dict, worker: str) -> dict:
    credenciales = extraer_credenciales(datos)
    if credenciales is None:
        return respuesta_error(400, "Se requieren 'usuario' y 'contraseña'", worker)

    fila = verificar_usuario(conn, *credenciales)
    if fila is None:
        return respuesta_error(401, "Usuario o contraseña incorrectos", worker)

    return respuesta_exito(
        200,
        {
            "mensaje": f"Inicio de sesión exitoso. ¡Bienvenido/a, {fila['usuario']}!",
            "usuario": fila["usuario"],
        },
        worker,
    )


def accion_listar_tareas(conn: sqlite3.Connection, usuario: sqlite3.Row, worker: str) -> dict:
    filas = conn.execute(
        "SELECT * FROM tareas WHERE usuario_id = ? ORDER BY id",
        (usuario["id"],),
    ).fetchall()
    tareas = [tarea_a_dict(f) for f in filas]
    return respuesta_exito(200, {"usuario": usuario["usuario"], "tareas": tareas}, worker)


def accion_crear_tarea(
    conn: sqlite3.Connection, usuario: sqlite3.Row, datos: dict, worker: str
) -> dict:
    titulo = str(datos.get("titulo", "")).strip()
    if not titulo:
        return respuesta_error(400, "El campo 'titulo' es obligatorio", worker)
    descripcion = str(datos.get("descripcion", "")).strip()

    # Permite simular tiempo de procesamiento pesado cuando se envían lotes en paralelo
    demora = float(datos.get("simular_trabajo", 0) or 0)
    if demora > 0:
        time.sleep(min(demora, 5.0))

    with db_lock:
        cursor = conn.execute(
            "INSERT INTO tareas (usuario_id, titulo, descripcion) VALUES (?, ?, ?)",
            (usuario["id"], titulo, descripcion),
        )
        conn.commit()
        nueva = obtener_tarea(conn, cursor.lastrowid, usuario["id"])

    return respuesta_exito(201, tarea_a_dict(nueva), worker)


def accion_actualizar_tarea(
    conn: sqlite3.Connection, usuario: sqlite3.Row, datos: dict, worker: str
) -> dict:
    tarea_id = datos.get("id")
    if not isinstance(tarea_id, int):
        return respuesta_error(400, "Se requiere un 'id' de tarea válido", worker)

    with db_lock:
        tarea = obtener_tarea(conn, tarea_id, usuario["id"])
        if tarea is None:
            return respuesta_error(404, "Tarea no encontrada", worker)

        titulo = str(datos.get("titulo", tarea["titulo"])).strip()
        if not titulo:
            return respuesta_error(400, "El campo 'titulo' no puede quedar vacío", worker)
        descripcion = str(datos.get("descripcion", tarea["descripcion"])).strip()
        completada = int(bool(datos.get("completada", tarea["completada"])))

        conn.execute(
            "UPDATE tareas SET titulo = ?, descripcion = ?, completada = ? WHERE id = ?",
            (titulo, descripcion, completada, tarea_id),
        )
        conn.commit()
        actualizada = obtener_tarea(conn, tarea_id, usuario["id"])

    return respuesta_exito(200, tarea_a_dict(actualizada), worker)


def accion_eliminar_tarea(
    conn: sqlite3.Connection, usuario: sqlite3.Row, datos: dict, worker: str
) -> dict:
    tarea_id = datos.get("id")
    if not isinstance(tarea_id, int):
        return respuesta_error(400, "Se requiere un 'id' de tarea válido", worker)

    with db_lock:
        if obtener_tarea(conn, tarea_id, usuario["id"]) is None:
            return respuesta_error(404, "Tarea no encontrada", worker)
        conn.execute("DELETE FROM tareas WHERE id = ?", (tarea_id,))
        conn.commit()

    return respuesta_exito(200, {"mensaje": f"Tarea {tarea_id} eliminada"}, worker)


def procesar_peticion(peticion: dict, worker: str) -> dict:
    """Despacha la petición recibida hacia la acción correspondiente."""
    accion = str(peticion.get("accion", "")).strip()
    datos = peticion.get("datos") if isinstance(peticion.get("datos"), dict) else {}

    with conectar_db() as conn:
        if accion == "registro":
            return accion_registro(conn, datos, worker)
        if accion == "login":
            return accion_login(conn, datos, worker)

        if accion in ("listar_tareas", "crear_tarea", "actualizar_tarea", "eliminar_tarea"):
            usuario = autenticar_peticion(conn, peticion)
            if usuario is None:
                return respuesta_error(401, "Credenciales inválidas o ausentes", worker)

            if accion == "listar_tareas":
                return accion_listar_tareas(conn, usuario, worker)
            if accion == "crear_tarea":
                return accion_crear_tarea(conn, usuario, datos, worker)
            if accion == "actualizar_tarea":
                return accion_actualizar_tarea(conn, usuario, datos, worker)
            if accion == "eliminar_tarea":
                return accion_eliminar_tarea(conn, usuario, datos, worker)

        return respuesta_error(400, f"Acción desconocida: '{accion}'", worker)


# ---------------------------------------------------------------------------
# Pool de Hilos Workers
# ---------------------------------------------------------------------------

def ciclo_worker(nombre_worker: str) -> None:
    """
    Bucle principal de cada hilo worker.
    Toma tareas de la cola de mensajes, las ejecuta y entrega el resultado.
    """
    while True:
        item = cola_tareas.get()
        if item is None:
            # Señal de parada para cerrar el worker ordenadamente
            cola_tareas.task_done()
            break

        num_tarea, direccion, peticion, futuro = item
        accion = peticion.get("accion", "desconocida")
        cliente_str = f"{direccion[0]}:{direccion[1]}"
        print(f"[{nombre_worker}] Procesando tarea #{num_tarea} ('{accion}') de {cliente_str}")

        try:
            resultado = procesar_peticion(peticion, nombre_worker)
            futuro.set_result(resultado)
            print(
                f"[{nombre_worker}] Tarea #{num_tarea} ('{accion}') completada "
                f"-> código {resultado['codigo']}"
            )
        except Exception as error:
            print(f"[{nombre_worker}] Error en tarea #{num_tarea}: {error}")
            futuro.set_result(
                respuesta_error(500, "Error interno en el worker", nombre_worker)
            )
        finally:
            cola_tareas.task_done()


def iniciar_workers(cantidad: int) -> list[threading.Thread]:
    """Crea e inicia el pool de hilos workers."""
    hilos = []
    for i in range(1, cantidad + 1):
        nombre = f"Worker-{i}"
        hilo = threading.Thread(target=ciclo_worker, args=(nombre,), name=nombre, daemon=True)
        hilo.start()
        hilos.append(hilo)
    print(f"[WORKERS] Pool de {cantidad} workers activos ({', '.join(h.name for h in hilos)}).")
    return hilos


# ---------------------------------------------------------------------------
# Comunicación por Socket TCP
# ---------------------------------------------------------------------------

def encolar_y_esperar(direccion, peticion: dict) -> dict:
    """Encola una tarea en la cola de mensajes y espera el resultado del worker."""
    num_tarea = next(contador_tareas)
    futuro: Future = Future()
    accion = peticion.get("accion", "desconocida")

    cola_tareas.put((num_tarea, direccion, peticion, futuro))
    print(
        f"[COLA] Tarea #{num_tarea} ('{accion}') encolada "
        f"(pendientes en cola: {cola_tareas.qsize()})"
    )
    return futuro.result()


def atender_cliente(conexion: socket.socket, direccion) -> None:
    """
    Atiende la conexión TCP de un cliente.
    Recibe mensajes JSON delimitados por salto de línea, los envía a la cola
    de workers y devuelve las respuestas por el mismo socket.
    """
    errores_desconexion = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)
    cliente_str = f"{direccion[0]}:{direccion[1]}"

    with conexion:
        archivo = conexion.makefile("r", encoding="utf-8", errors="replace")
        while True:
            try:
                linea = archivo.readline()
            except errores_desconexion:
                print(f"[CONEXION] El cliente {cliente_str} cerró la conexión abruptamente.")
                break
            except OSError:
                break

            if not linea:
                break

            linea = linea.strip()
            if not linea:
                continue

            try:
                peticion = json.loads(linea)
                if not isinstance(peticion, dict):
                    raise ValueError("El mensaje debe ser un objeto JSON")
                respuesta = encolar_y_esperar(direccion, peticion)
            except json.JSONDecodeError:
                respuesta = respuesta_error(400, "Formato JSON inválido", "Coordinador")
            except ValueError as error:
                respuesta = respuesta_error(400, str(error), "Coordinador")

            datos_envio = (json.dumps(respuesta, ensure_ascii=False) + "\n").encode("utf-8")
            try:
                conexion.sendall(datos_envio)
            except errores_desconexion:
                print(f"[CONEXION] El cliente {cliente_str} se desconectó antes de recibir respuesta.")
                break


def inicializar_socket(host: str = HOST, port: int = PORT) -> socket.socket:
    """Crea, configura y pone a escuchar el socket TCP principal."""
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Timeout de 1 segundo para que el bucle accept() responda a Ctrl+C en Windows
    servidor.settimeout(1.0)

    try:
        servidor.bind((host, port))
    except OSError as error:
        print(f"[SOCKET] No se pudo iniciar el servidor en {host}:{port} -> {error}")
        print("[SOCKET] Verificá que el puerto no esté en uso o indicá otro puerto.")
        sys.exit(1)

    servidor.listen()
    print(f"[SOCKET] Servidor escuchando en {host}:{port}...")
    return servidor


def iniciar_servidor() -> None:
    init_db()
    iniciar_workers(NUM_WORKERS)
    servidor = inicializar_socket(HOST, PORT)

    try:
        while True:
            try:
                conexion, direccion = servidor.accept()
            except socket.timeout:
                continue

            # Cada cliente es recibido en su propio hilo para no bloquear nuevas conexiones
            hilo_cliente = threading.Thread(
                target=atender_cliente,
                args=(conexion, direccion),
                daemon=True,
            )
            hilo_cliente.start()
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Deteniendo servidor (Ctrl+C)...")
    finally:
        for _ in range(NUM_WORKERS):
            cola_tareas.put(None)
        servidor.close()
        print("[SERVIDOR] Socket cerrado y workers detenidos.")


if __name__ == "__main__":
    iniciar_servidor()
