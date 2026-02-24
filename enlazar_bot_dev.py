import telebot
import MySQLdb
import sys

# CONFIGURACIÓN PRODUCCIÓN
DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
DB_USER = "baquiasoft"
DB_PASS = "Ataraxia123*/"
DB_NAME = "baquiasoft$energix_360"
TOKEN = "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"

bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def enviar_bienvenida(message):
    # Esto te dirá en la consola si el bot recibió el mensaje
    print(f"DEBUG 🔍: Recibido /start de {message.chat.first_name} (ID: {message.chat.id})")
    
    markup = telebot.types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    boton = telebot.types.KeyboardButton(text="📱 Compartir mi número", request_contact=True)
    markup.add(boton)
    
    bot.reply_to(message, "👋 Hola. Presiona el botón abajo para vincular tu cuenta:", reply_markup=markup)

@bot.message_handler(content_types=['contact'])
def recibir_contacto(message):
    if message.contact:
        tel = message.contact.phone_number.replace("+", "").replace(" ", "")
        # Ajustamos para buscar los últimos 9 o 10 dígitos por si acaso
        tel_busqueda = tel[-9:] 
        chat_id = message.chat.id

        print(f"DEBUG 📞: Procesando contacto {tel}. Buscando: %{tel_busqueda}")

        try:
            conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
            cur = conn.cursor(MySQLdb.cursors.DictCursor) # Usamos DictCursor para ver nombres
            
            # Buscamos TODOS los usuarios que tengan ese número (por si hay duplicados)
            cur.execute("SELECT id, nombre, empresa_id FROM usuarios WHERE telefono LIKE %s", (f"%{tel_busqueda}",))
            usuarios = cur.fetchall()

            if len(usuarios) == 0:
                print("DEBUG ❌: No se encontró nadie en la BD.")
                bot.reply_to(message, f"El número {tel} no existe en Energix 360.")
            elif len(usuarios) > 1:
                print(f"DEBUG ⚠️: Se encontraron {len(usuarios)} usuarios con el mismo número!")
                bot.reply_to(message, "Error: Tu número está duplicado en el sistema. Contacta a soporte.")
            else:
                user = usuarios[0]
                print(f"DEBUG ✅: Vinculando a {user['nombre']}...")
                cur.execute("UPDATE usuarios SET telegram_id = %s WHERE id = %s", (str(chat_id), user['id']))
                conn.commit()
                bot.reply_to(message, f"✅ ¡Vínculo exitoso, {user['nombre']}!")

            cur.close()
            conn.close()
        except Exception as e:
            print(f"DEBUG 🔥: Error de BD: {e}")
            bot.reply_to(message, "Error interno.")

print("🚀 BOT CORRIENDO... Mira esta pantalla cuando escribas en Telegram.")
bot.infinity_polling()