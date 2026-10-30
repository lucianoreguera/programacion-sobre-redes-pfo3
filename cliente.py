# ---------------------------------------------------------------------------
# Reguera, Luciano Oscar
# Comisión D
# 30-10-2026
# ---------------------------------------------------------------------------

"""
Cliente de consola por Socket TCP para el Sistema Distribuido de Gestión de Tareas.

Envía tareas/operaciones en formato JSON por socket TCP al servidor,
donde son encoladas y distribuidas a un pool de hilos workers, y recibe
los resultados junto con el identificador del worker que procesó cada tarea.

Uso:
    python cliente.py                  # conecta a localhost:5000
    python cliente.py otro-host:6000   # conecta a otro host/puerto

Dentro de cualquier operación, presionar Ctrl+C la cancela
y vuelve al menú sin realizar ningún cambio.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from getpass import getpass
import json
import socket
import sys

TIMEOUT = 10  # segundos


def parsear_destino() -> tuple[str, int]:
    """Obtiene (host, puerto) desde los argumentos de línea de comandos."""
    if len(sys.argv) < 2:
        return "localhost", 5000

    arg = sys.argv[1].strip()
    for prefijo in ("tcp://", "http://", "https://"):
        if arg.startswith(prefijo):
            arg = arg[len(prefijo):]
    arg = arg.rstrip("/")

    if ":" in arg:
        host, puerto_str = arg.rsplit(":", 1)
        return (host or "localhost"), int(puerto_str)

    if len(sys.argv) >= 3 and sys.argv[2].isdigit():
        return arg, int(sys.argv[2])

    return ( ("localhost", int(arg)) if arg.isdigit() else (arg, 5000) )


HOST, PORT = parsear_destino()


# ---------------------------------------------------------------------------
# Comunicación con el servidor por Socket TCP
# ---------------------------------------------------------------------------

def llamar_servidor(accion: str, datos: dict | None = None, auth: tuple[str, str] | None = None):
    """
    Envía una tarea/petición al servidor por socket TCP y espera la respuesta.
    Devuelve (código, datos_respuesta, nombre_worker) o (None, None, None) si falla la conexión.
    """
    peticion: dict = {"accion": accion, "datos": datos or {}}
    if auth is not None:
        peticion["auth"] = {"usuario": auth[0], "contraseña": auth[1]}

    mensaje = (json.dumps(peticion, ensure_ascii=False) + "\n").encode("utf-8")

    try:
        with socket.create_connection((HOST, PORT), timeout=TIMEOUT) as sock:
            sock.sendall(mensaje)
            archivo = sock.makefile("r", encoding="utf-8", errors="replace")
            linea = archivo.readline()
    except OSError:
        print(f"\n❌ No se pudo conectar con el servidor en {HOST}:{PORT}. ¿Está corriendo servidor.py?")
        return None, None, None

    if not linea:
        print("\n❌ El servidor cerró la conexión sin enviar respuesta.")
        return None, None, None

    try:
        respuesta = json.loads(linea)
    except json.JSONDecodeError:
        print("\n❌ Respuesta inválida del servidor.")
        return None, None, None

    codigo = respuesta.get("codigo", 500)
    cuerpo = respuesta.get("datos") if isinstance(respuesta.get("datos"), dict) else {}
    worker = respuesta.get("worker", "Worker")
    return codigo, cuerpo, worker


def mostrar_error(datos: dict, worker: str | None = None) -> None:
    sufijo = f" [{worker}]" if worker else ""
    print(f"❌ {datos.get('error', 'Error inesperado')}{sufijo}")


# ---------------------------------------------------------------------------
# Entrada por teclado
# ---------------------------------------------------------------------------

def encabezado(titulo: str) -> None:
    print(f"\n--- {titulo} ---")
    print("    (Ctrl+C para cancelar y volver al menú)")


def pedir_texto(mensaje: str, obligatorio: bool = True) -> str:
    while True:
        valor = input(mensaje).strip()
        if valor or not obligatorio:
            return valor
        print("   Este campo es obligatorio (Ctrl+C para cancelar).")


def pedir_contrasena(mensaje: str = "Contraseña: ") -> str:
    # getpass oculta lo que se escribe; si la entrada no es una terminal, usa input.
    leer = getpass if sys.stdin.isatty() else input
    while True:
        valor = leer(mensaje)
        if valor:
            return valor
        print("   Este campo es obligatorio (Ctrl+C para cancelar).")


def confirmar(mensaje: str) -> bool:
    """Pregunta s/n hasta obtener una respuesta válida. Devuelve True si la respuesta es 's'."""
    while True:
        valor = pedir_texto(f"{mensaje} (s/n): ").lower()
        if valor in ("s", "n"):
            return valor == "s"
        print("   Respondé 's' o 'n' (Ctrl+C para cancelar).")


# ---------------------------------------------------------------------------
# Acciones: usuarios
# ---------------------------------------------------------------------------

def registrarse() -> None:
    encabezado("Registro de usuario")
    usuario = pedir_texto("Usuario: ")
    while True:
        contrasena = pedir_contrasena()
        if pedir_contrasena("Repetir contraseña: ") == contrasena:
            break
        print("❌ Las contraseñas no coinciden. Probá de nuevo.")

    codigo, datos, worker = llamar_servidor(
        "registro", datos={"usuario": usuario, "contraseña": contrasena}
    )
    if codigo == 201:
        print(f"✅ {datos['mensaje']} (procesado por {worker}). Ya podés iniciar sesión.")
    elif codigo is not None:
        mostrar_error(datos, worker)


def iniciar_sesion():
    """Devuelve la tupla (usuario, contraseña) si el login es correcto, o None."""
    encabezado("Iniciar sesión")
    usuario = pedir_texto("Usuario: ")
    contrasena = pedir_contrasena()

    codigo, datos, worker = llamar_servidor(
        "login", datos={"usuario": usuario, "contraseña": contrasena}
    )
    if codigo == 200:
        print(f"✅ {datos['mensaje']} (atendido por {worker})")
        return usuario, contrasena
    if codigo is not None:
        mostrar_error(datos, worker)
    return None


# ---------------------------------------------------------------------------
# Acciones: tareas (requieren credenciales)
# ---------------------------------------------------------------------------

def obtener_tareas(auth: tuple[str, str]):
    """Devuelve (lista_de_tareas, worker) del usuario, o (None, None) si hubo un error."""
    codigo, datos, worker = llamar_servidor("listar_tareas", auth=auth)
    if codigo == 200:
        return datos["tareas"], worker
    if codigo is not None:
        mostrar_error(datos, worker)
    return None, None


def marca(tarea: dict) -> str:
    return "✔" if tarea["completada"] else " "


def elegir_tarea(auth: tuple[str, str]):
    """Muestra las tareas y pide el ID de una hasta que sea válido. Devuelve la tarea, o None si no hay."""
    tareas, _ = obtener_tareas(auth)
    if tareas is None:
        return None
    if not tareas:
        print("   No tenés tareas cargadas.")
        return None

    for t in tareas:
        print(f"   #{t['id']:<3} [{marca(t)}] {t['titulo']}")
    por_id = {t["id"]: t for t in tareas}
    while True:
        valor = pedir_texto("ID de la tarea: ")
        if valor.isdigit() and int(valor) in por_id:
            return por_id[int(valor)]
        print(f"   ❌ No existe una tarea con ID '{valor}'. Probá de nuevo (Ctrl+C para cancelar).")


def ver_tareas(auth: tuple[str, str]) -> None:
    tareas, worker = obtener_tareas(auth)
    if tareas is None:
        return

    print(f"\n--- Tareas de {auth[0]} ({len(tareas)}) [consultado por {worker}] ---")
    if not tareas:
        print("   No tenés tareas cargadas.")
        return
    for t in tareas:
        print(f"  [{marca(t)}] #{t['id']:<3} {t['titulo']}")
        if t["descripcion"]:
            print(f"          {t['descripcion']}")
        print(f"          creada: {t['creada_en']}")


def crear_tarea(auth: tuple[str, str]) -> None:
    encabezado("Nueva tarea")
    titulo = pedir_texto("Título: ")
    descripcion = pedir_texto("Descripción (Enter para dejarla vacía): ", obligatorio=False)

    codigo, datos, worker = llamar_servidor(
        "crear_tarea",
        datos={"titulo": titulo, "descripcion": descripcion},
        auth=auth,
    )
    if codigo == 201:
        print(f"✅ Tarea #{datos['id']} creada (procesada por {worker}).")
    elif codigo is not None:
        mostrar_error(datos, worker)


def cambiar_estado(auth: tuple[str, str]) -> None:
    encabezado("Marcar tarea como completada / pendiente")
    tarea = elegir_tarea(auth)
    if tarea is None:
        return
    nuevo_estado = "pendiente" if tarea["completada"] else "completada"
    if not confirmar(f"¿Marcar '{tarea['titulo']}' como {nuevo_estado}?"):
        print("   Operación cancelada.")
        return

    codigo, datos, worker = llamar_servidor(
        "actualizar_tarea",
        datos={"id": tarea["id"], "completada": not tarea["completada"]},
        auth=auth,
    )
    if codigo == 200:
        print(f"✅ Tarea #{tarea['id']} marcada como {nuevo_estado} (procesada por {worker}).")
    elif codigo is not None:
        mostrar_error(datos, worker)


def editar_tarea(auth: tuple[str, str]) -> None:
    encabezado("Editar tarea")
    tarea = elegir_tarea(auth)
    if tarea is None:
        return

    print("   (Enter para mantener el valor actual)")
    cambios: dict = {"id": tarea["id"]}
    titulo = pedir_texto(f"Título [{tarea['titulo']}]: ", obligatorio=False)
    if titulo:
        cambios["titulo"] = titulo
    descripcion = pedir_texto(f"Descripción [{tarea['descripcion']}]: ", obligatorio=False)
    if descripcion:
        cambios["descripcion"] = descripcion
    if len(cambios) == 1:
        print("   No se hicieron cambios.")
        return

    codigo, datos, worker = llamar_servidor("actualizar_tarea", datos=cambios, auth=auth)
    if codigo == 200:
        print(f"✅ Tarea #{tarea['id']} actualizada (procesada por {worker}).")
    elif codigo is not None:
        mostrar_error(datos, worker)


def eliminar_tarea(auth: tuple[str, str]) -> None:
    encabezado("Eliminar tarea")
    tarea = elegir_tarea(auth)
    if tarea is None:
        return
    if not confirmar(f"¿Seguro que querés eliminar '{tarea['titulo']}'?"):
        print("   Operación cancelada.")
        return

    codigo, datos, worker = llamar_servidor(
        "eliminar_tarea", datos={"id": tarea["id"]}, auth=auth
    )
    if codigo == 200:
        print(f"✅ {datos['mensaje']} (procesada por {worker}).")
    elif codigo is not None:
        mostrar_error(datos, worker)


def enviar_lote_tareas(auth: tuple[str, str]) -> None:
    """
    Envía múltiples tareas en paralelo por sockets TCP para demostrar cómo el
    servidor las encola y las distribuye concurrentemente entre sus workers.
    """
    encabezado("Enviar lote de tareas en paralelo (distribución en Workers)")
    prefijo = pedir_texto("Título base para las tareas [Tarea distribuida]: ", obligatorio=False)
    if not prefijo:
        prefijo = "Tarea distribuida"

    while True:
        cant_str = pedir_texto("Cantidad de tareas a enviar en simultáneo (1-20) [4]: ", obligatorio=False)
        if not cant_str:
            cantidad = 4
            break
        if cant_str.isdigit() and 1 <= int(cant_str) <= 20:
            cantidad = int(cant_str)
            break
        print("   ❌ Ingresá un número entre 1 y 20.")

    print(f"\n🚀 Enviando {cantidad} tareas concurrentes por socket a {HOST}:{PORT}...")

    def _enviar_una(indice: int):
        titulo = f"{prefijo} #{indice}"
        descripcion = f"Enviada en lote concurrente (ítem {indice}/{cantidad})"
        codigo, datos, worker = llamar_servidor(
            "crear_tarea",
            datos={"titulo": titulo, "descripcion": descripcion, "simular_trabajo": 0.3},
            auth=auth,
        )
        return indice, codigo, datos, worker

    with ThreadPoolExecutor(max_workers=cantidad) as pool:
        futuros = [pool.submit(_enviar_una, i) for i in range(1, cantidad + 1)]
        for futuro in as_completed(futuros):
            indice, codigo, datos, worker = futuro.result()
            if codigo == 201 and datos:
                print(
                    f"   ✅ [Ítem {indice}] Tarea #{datos['id']} '{datos['titulo']}' "
                    f"-> procesada por {worker}"
                )
            elif codigo is not None and datos:
                mostrar_error(datos, worker)


# ---------------------------------------------------------------------------
# Menús
# ---------------------------------------------------------------------------

def ejecutar(accion, *args):
    """Ejecuta una acción; si el usuario la cancela con Ctrl+C, vuelve al menú sin hacer nada."""
    try:
        return accion(*args)
    except KeyboardInterrupt:
        print("\n↩  Operación cancelada. Volviendo al menú.")
        return None


def menu_tareas(auth: tuple[str, str]) -> bool:
    """Menú para el usuario autenticado. Devuelve False si eligió salir del programa."""
    opciones = {
        "1": ver_tareas,
        "2": crear_tarea,
        "3": cambiar_estado,
        "4": editar_tarea,
        "5": eliminar_tarea,
        "6": enviar_lote_tareas,
    }
    while True:
        print(f"\n===== Tareas ({auth[0]}) =====")
        print("1. Ver mis tareas")
        print("2. Nueva tarea")
        print("3. Marcar completada / pendiente")
        print("4. Editar tarea")
        print("5. Eliminar tarea")
        print("6. Enviar lote de tareas (prueba de Workers)")
        print("7. Cerrar sesión")
        print("0. Salir")
        opcion = input("Opción: ").strip()

        if opcion in opciones:
            ejecutar(opciones[opcion], auth)
        elif opcion == "7":
            print("👋 Sesión cerrada.")
            return True
        elif opcion == "0":
            return False
        else:
            print("Opción inválida.")


def main() -> None:
    print("=========================================")
    print("   Sistema Distribuido de Tareas (TCP)")
    print(f"   Servidor Socket: {HOST}:{PORT}")
    print("=========================================")

    while True:
        print("\n===== Menú principal =====")
        print("1. Registrarse")
        print("2. Iniciar sesión")
        print("0. Salir")
        opcion = input("Opción: ").strip()

        if opcion == "1":
            ejecutar(registrarse)
        elif opcion == "2":
            auth = ejecutar(iniciar_sesion)
            if auth and not menu_tareas(auth):
                break
        elif opcion == "0":
            break
        else:
            print("Opción inválida.")

    print("¡Hasta luego!")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\n¡Hasta luego!")
