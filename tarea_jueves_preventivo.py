import MySQLdb
import requests
import datetime
import sys

# --- TUS CREDENCIALES (CÁMBIALAS POR LAS REALES DE PYTHONANYWHERE) ---
DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
DB_USER = "baquiasof"
DB_PASS = "Ataraxia123*/"
DB_NAME = "baquiasoft$energix_v2"
TOKEN = "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"

def enviar_telegram(chat_id, mensaje):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        # Parse mode Markdown permite negritas (*texto*) y cursivas (_texto_)
        requests.post(url, data={"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"})
        print(f"✅ Mensaje enviado a {chat_id}")
    except Exception as e:
        print(f"❌ Error enviando a {chat_id}: {e}")

def procesar_alertas():
    # 1. FECHA Y HORA ACTUAL (Hora Colombia es UTC-5, ajusta si es necesario)
    hoy = datetime.date.today()
    dia_semana = hoy.weekday() # 0=Lunes, 1=Martes, 2=Miércoles, 3=Jueves, 4=Viernes...
    
    print(f"🚀 INICIANDO AUDITORÍA DE GAS - {hoy}")

    try:
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        cur = conn.cursor(MySQLdb.cursors.DictCursor)

        # 2. SELECCIONAR USUARIOS CON TELEGRAM (Operadores o Admin)
        # Filtramos para que solo le llegue a quienes tienen telegram_id configurado
        sql_users = """
            SELECT nombre, telegram_id, empresa_id 
            FROM usuarios 
            WHERE telegram_id IS NOT NULL AND telegram_id != '' 
            AND estatus = 'activo'
        """
        cur.execute(sql_users)
        operadores = cur.fetchall()

        for op in operadores:
            empresa_id = op['empresa_id']
            chat_id = op['telegram_id']
            nombre = op['nombre']

            # 3. BUSCAR GRANJAS ACTIVAS DE ESTA EMPRESA
            # Solo traemos lotes activos ('ACTIVO')
            sql_granjas = """
                SELECT ubicacion, dias_operacion, lote, `nivel tk-1` as nivel
                FROM cardex_glp 
                WHERE estatus_lote = 'ACTIVO' 
                AND id_empresa = %s
            """
            cur.execute(sql_granjas, (empresa_id,))
            granjas = cur.fetchall()

            if not granjas:
                continue # Si no tiene granjas activas, pasamos al siguiente usuario

            # 4. CONSTRUCCIÓN DEL MENSAJE (LÓGICA INTELIGENTE)
            
            # --- Encabezado ---
            mensaje = f"👋 Hola *{nombre}*,\n"
            mensaje += f"📅 *REPORTE DE ESTADO - {hoy}*\n"
            mensaje += "Aquí están tus lotes activos actualmente:\n\n"

            # --- Cuerpo (Lista de Granjas) ---
            alerta_nivel_critico = False
            
            for g in granjas:
                nivel = g['nivel'] if g['nivel'] is not None else 0
                dias = g['dias_operacion']
                ubi = g['ubicacion']
                
                # Icono de nivel según porcentaje
                icono_nivel = "🟢"
                if nivel < 30: 
                    icono_nivel = "🔴"
                    alerta_nivel_critico = True
                elif nivel < 50: 
                    icono_nivel = "🟡"

                mensaje += f"📍 *{ubi}*\n"
                mensaje += f"   🔥 Días Calefacción: *{dias}*\n"
                mensaje += f"   {icono_nivel} Nivel Actual: *{nivel}%*\n\n"

            # --- 5. LOGICA DEL JUEVES (El Cerebro del Script) ---
            if dia_semana == 3: # JUEVES
                mensaje += "⚠️🚨 *¡AVISO IMPORTANTE DE JUEVES!* 🚨⚠️\n\n"
                mensaje += "Mañana es viernes. Por favor revisa tus niveles AHORA.\n"
                mensaje += "1️⃣ *Registra tu consumo hoy* para validar niveles.\n"
                mensaje += "2️⃣ Recuerda que el proveedor toma *24 horas* en despachar.\n"
                mensaje += "3️⃣ Los fines de semana solo despachan hasta las *11:00 AM*.\n\n"
                mensaje += "🚫 _¡No te arriesgues a quedarte sin gas el fin de semana!_ 🚫"
            
            elif alerta_nivel_critico:
                # Si no es jueves, pero hay tanques bajos
                mensaje += "⚠️ *ALERTA DE NIVEL BAJO DETECTADA*\n"
                mensaje += "Tienes tanques por debajo del 30%. Solicita tu pedido con anticipación.\n"
                mensaje += "🚛 Recuerda: Tiempo de entrega = 24 horas."
            
            else:
                # Mensaje estándar para otros días (Lunes, Martes, etc.)
                mensaje += "ℹ️ *Recordatorio:*\n"
                mensaje += "Registra tu consumo diario en la App para mantener el control.\n"
                mensaje += "🚛 Tiempo de suministro: 24 horas."

            # 6. ENVIAR
            enviar_telegram(chat_id, mensaje)

        cur.close()
        conn.close()
        print("🏁 Proceso de notificaciones finalizado.")

    except Exception as e:
        print(f"🔥 Error crítico en el script: {e}")

if __name__ == "__main__":
    procesar_alertas()