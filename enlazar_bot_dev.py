# enlazar_bot_dev.py
import telebot
import MySQLdb
import threading
import sys
import time
from telebot.apihelper import ApiTelegramException

# =======================================================
# CONFIGURACIÓN DE ENTORNO Y BASE DE DATOS
# =======================================================
# Cambia a "produccion" cuando subas este archivo a PythonAnywhere
ENTORNO = "desarrollo"

if ENTORNO == "desarrollo":
    DB_HOST = "127.0.0.1"
    DB_USER = "root"
    DB_PASS = ""
    DB_NAME = "energix_360"  # <-- Ajusta si el nombre difiere en tu XAMPP local
else:
    DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
    DB_USER = "baquiasoft"
    DB_PASS = "Ataraxia123*/"
    DB_NAME = "baquiasoft$energix_v2"

# Token original del módulo GLP
TOKEN_GLP = "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"

# NUEVO Token para Transporte Especial
TOKEN_ESPECIAL = "8841682239:AAFOj8TpeOW4ulhIkNoIyGaTZ2MLlI9ydVo"

bot_glp = telebot.TeleBot(TOKEN_GLP)
bot_especial = telebot.TeleBot(TOKEN_ESPECIAL)

# Limpiar Webhooks antes de iniciar Polling (Evita conflictos silenciosos de red)
try:
    bot_glp.remove_webhook()
    print("DEBUG 🧹 [GLP]: Webhook eliminado correctamente.")
except Exception as e:
    print(f"DEBUG ⚠️ [GLP]: Error al eliminar webhook: {e}")

try:
    bot_especial.remove_webhook()
    print("DEBUG 🧹 [ESPECIAL]: Webhook eliminado correctamente.")
except Exception as e:
    print(f"DEBUG ⚠️ [ESPECIAL]: Error al eliminar webhook: {e}")

# =======================================================
# LÓGICA BOT 1: GLP
# =======================================================
@bot_glp.message_handler(commands=['start'])
def enviar_bienvenida_glp(message):
    print(f"DEBUG 🔍 [GLP]: Recibido /start de {message.chat.first_name} (ID: {message.chat.id}) a las {time.strftime('%X')}")
    markup = telebot.types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    boton = telebot.types.KeyboardButton(text="📱 Compartir mi número", request_contact=True)
    markup.add(boton)
    bot_glp.reply_to(message, "👋 Hola. Presiona el botón abajo para vincular tu cuenta de GLP:", reply_markup=markup)

@bot_glp.message_handler(content_types=['contact'])
def recibir_contacto_glp(message):
    procesar_contacto(message, bot_glp, "GLP")

# =======================================================
# LÓGICA BOT 2: TRANSPORTE ESPECIAL
# =======================================================
@bot_especial.message_handler(commands=['start'])
def enviar_bienvenida_especial(message):
    print(f"DEBUG 🔍 [ESPECIAL]: Recibido /start de {message.chat.first_name} (ID: {message.chat.id}) a las {time.strftime('%X')}")
    markup = telebot.types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    boton = telebot.types.KeyboardButton(text="📱 Compartir mi número", request_contact=True)
    markup.add(boton)
    bot_especial.reply_to(message, "🚐 Hola. Presiona el botón abajo para vincular tu cuenta de Transporte Especial:", reply_markup=markup)

@bot_especial.message_handler(content_types=['contact'])
def recibir_contacto_especial(message):
    procesar_contacto(message, bot_especial, "Transporte Especial")

# =======================================================
# FUNCIÓN COMPARTIDA DE BASE DE DATOS (Con Reconexión)
# =======================================================
def procesar_contacto(message, bot_instance, modulo_nombre):
    if message.contact:
        tel = message.contact.phone_number.replace("+", "").replace(" ", "")
        tel_busqueda = tel[-9:] 
        chat_id = message.chat.id

        print(f"DEBUG 📞 [{modulo_nombre}]: Procesando contacto {tel}. Buscando: %{tel_busqueda}")

        conn = None
        cur = None
        try:
            conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME, connect_timeout=10)
            cur = conn.cursor(MySQLdb.cursors.DictCursor) 
            
            cur.execute("SELECT id, nombre, empresa_id FROM usuarios WHERE telefono LIKE %s", (f"%{tel_busqueda}",))
            usuarios = cur.fetchall()

            if len(usuarios) == 0:
                print(f"DEBUG ❌ [{modulo_nombre}]: No se encontró nadie en la BD.")
                bot_instance.reply_to(message, f"El número {tel} no existe en Energix 360.")
            elif len(usuarios) > 1:
                print(f"DEBUG ⚠️ [{modulo_nombre}]: Se encontraron {len(usuarios)} usuarios duplicados.")
                bot_instance.reply_to(message, "Error: Tu número está duplicado en el sistema. Contacta a soporte.")
            else:
                user = usuarios[0]
                print(f"DEBUG ✅ [{modulo_nombre}]: Vinculando a {user['nombre']}...")
                cur.execute("UPDATE usuarios SET telegram_id = %s WHERE id = %s", (str(chat_id), user['id']))
                conn.commit()
                bot_instance.reply_to(message, f"✅ ¡Vínculo exitoso en {modulo_nombre} para {user['nombre']}!")

        except MySQLdb.Error as e:
            print(f"DEBUG 🔥 [{modulo_nombre}]: Error de BD MySQL: {e}")
            bot_instance.reply_to(message, "Error interno de servidor al conectar con la base de datos.")
        except Exception as e:
            print(f"DEBUG 🔥 [{modulo_nombre}]: Error general: {e}")
            bot_instance.reply_to(message, "Ocurrió un error inesperado.")
        finally:
            if cur:
                try: cur.close()
                except: pass
            if conn:
                try: conn.close()
                except: pass

# =======================================================
# MOTORES DE EJECUCIÓN EN PARALELO (HILOS CON TOLERANCIA)
# =======================================================
def correr_bot_glp():
    print("🚀 BOT GLP CORRIENDO...")
    while True:
        try:
            bot_glp.infinity_polling(timeout=60, long_polling_timeout=30, skip_pending=True)
        except ApiTelegramException as e:
            print(f"DEBUG 🔥 [GLP]: ApiTelegramException: {e}. Reconectando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"DEBUG 🔥 [GLP]: Excepción general en polling: {e}. Reconectando en 5s...")
            time.sleep(5)

def correr_bot_especial():
    print("🚐 BOT TRANSPORTE ESPECIAL CORRIENDO...")
    while True:
        try:
            bot_especial.infinity_polling(timeout=60, long_polling_timeout=30, skip_pending=True)
        except ApiTelegramException as e:
            print(f"DEBUG 🔥 [ESPECIAL]: ApiTelegramException: {e}. Reconectando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"DEBUG 🔥 [ESPECIAL]: Excepción general en polling: {e}. Reconectando en 5s...")
            time.sleep(5)

if __name__ == "__main__":
    print(f"Iniciando servicios Telegram BQA-ONE (Entorno: {ENTORNO.upper()})...")
    
    hilo_glp = threading.Thread(target=correr_bot_glp)
    hilo_especial = threading.Thread(target=correr_bot_especial)
    
    hilo_glp.daemon = True
    hilo_especial.daemon = True

    hilo_glp.start()
    hilo_especial.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo servicios de Telegram BQA-ONE...")
        sys.exit(0)