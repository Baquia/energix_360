import telebot
import MySQLdb  # Esta es la librería que ya tienes instalada
import sys

# --- CONFIGURACIÓN PARA TU XAMPP (LOCAL) ---
DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
DB_USER = "baquiasof"      # Usuario por defecto de XAMPP
DB_PASS = "Metanoia765/*"          # Por defecto en XAMPP es vacío
DB_NAME = "baquiasoft$energix_360"

# TU TOKEN DE TELEGRAM
TOKEN = "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"

print("🚀 Iniciando Bot en modo PRODUCCION...")

# Conectamos con Telegram
try:
    bot = telebot.TeleBot(TOKEN)
    print("✅ Conexión con Telegram exitosa.")
except Exception as e:
    print(f"❌ Error conectando con Telegram: {e}")
    sys.exit()

# 1. Cuando el usuario escribe /start
@bot.message_handler(commands=['start'])
def enviar_bienvenida(message):
    print(f"➡️  Usuario {message.chat.first_name} envió /start")
    
    # Creamos el botón especial para pedir el contacto
    markup = telebot.types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    boton = telebot.types.KeyboardButton(text="📱 Compartir mi número", request_contact=True)
    markup.add(boton)
    
    bot.reply_to(message, "👋 Hola. Para recibir tus alertas de gas, presiona el botón abajo:", reply_markup=markup)

# 2. Cuando el usuario comparte su contacto
@bot.message_handler(content_types=['contact'])
def recibir_contacto(message):
    if message.contact:
        telefono_original = message.contact.phone_number
        chat_id = message.chat.id
        usuario_tg = message.chat.first_name
        
        # LIMPIEZA: Quitamos el '+' y espacios para comparar solo números
        # Ejemplo: '+57300...' se convierte en '57300...'
        telefono_limpio = telefono_original.replace("+", "").replace(" ", "")
        # Tomamos los últimos 10 dígitos (para evitar problemas con el código de país 57)
        telefono_para_buscar = telefono_limpio[-10:]

        print(f"\n📩 Recibido de Telegram: {telefono_original} (Buscaremos coincidencias con: ...{telefono_para_buscar})")

        try:
            # Conexión a tu Base de Datos Local
            conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
            cur = conn.cursor()
            
            # Buscamos el usuario usando LIKE para encontrar el número al final
            query = "SELECT id, nombre FROM usuarios WHERE telefono LIKE %s"
            cur.execute(query, (f"%{telefono_para_buscar}",))
            
            usuario_db = cur.fetchone()

            if usuario_db:
                id_usuario = usuario_db[0]
                nombre_usuario = usuario_db[1]
                
                print(f"✅ ¡MATCH ENCONTRADO! Es: {nombre_usuario} (ID BD: {id_usuario})")
                
                # GUARDAMOS EL CHAT_ID EN LA BASE DE DATOS
                cur.execute("UPDATE usuarios SET telegram_id = %s WHERE id = %s", (str(chat_id), id_usuario))
                conn.commit()
                print("💾 Guardado en base de datos local.")
                
                bot.reply_to(message, f"✅ ¡Listo {nombre_usuario}! Tu Telegram ha sido vinculado exitosamente.", reply_markup=telebot.types.ReplyKeyboardRemove())
            else:
                print("❌ No se encontró ese número en la tabla 'usuarios'.")
                bot.reply_to(message, f"❌ El número {telefono_original} no está registrado en nuestro sistema. Contacta al administrador.", reply_markup=telebot.types.ReplyKeyboardRemove())
            
            cur.close()
            conn.close()

        except Exception as e:
            print(f"🔥 Error de Base de Datos: {e}")
            bot.reply_to(message, "Ocurrió un error interno en el servidor.")

print("🎧 Bot escuchando... (Ve a Telegram y dale /start)")
# BORRA: bot.polling()
# PON ESTO EN SU LUGAR:
bot.infinity_polling()