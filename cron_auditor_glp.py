# -*- coding: utf-8 -*-
import MySQLdb
import requests
from datetime import datetime, timedelta

# ================= TUS DATOS DE PRODUCCIÓN =================
DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
DB_USER = "baquiasoft"
DB_PASS = "Ataraxia123*/"
DB_NAME = "baquiasoft$energix_360"
TOKEN_TELEGRAM = "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"
# ==========================================================

def enviar_telegram(chat_id, mensaje):
    url = f"https://api.telegram.org/bot{TOKEN_TELEGRAM}/sendMessage"
    data = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=data, timeout=5)
    except Exception as e:
        print(f"Error enviando a {chat_id}: {e}")

def auditar_granjas():
    print(f"Iniciando auditoría GLP: {datetime.now()}")
    hoy = datetime.now().date()
    es_viernes = datetime.now().weekday() == 4 # 0=Lunes, 4=Viernes

    try:
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        cur = conn.cursor(MySQLdb.cursors.DictCursor)

        # 1. CONSULTA OPTIMIZADA: Calculamos MIN y MAX en un solo paso rápido sin subconsultas pesadas
        query_lotes = """
            SELECT 
                id_empresa, 
                empresa, 
                ubicacion, 
                lote, 
                MAX(fecha) as ultima_operacion,
                MIN(fecha) as fecha_inicio
            FROM cardex_glp
            WHERE estatus_lote = 'ACTIVO'
            GROUP BY id_empresa, empresa, ubicacion, lote
        """
        cur.execute(query_lotes)
        lotes_activos = cur.fetchall()

        # Diccionario para agrupar alertas por empresa
        alertas_por_empresa = {}

        for row in lotes_activos:
            emp_id = row['id_empresa']
            if emp_id not in alertas_por_empresa:
                alertas_por_empresa[emp_id] = {
                    'nombre': row['empresa'],
                    'alertas_frecuencia': [],
                    'alertas_vencidos': []
                }

            ultima_op = row['ultima_operacion']
            inicio_op = row['fecha_inicio']
            
            dias_sin_registro = (hoy - ultima_op).days if ultima_op else 0
            dias_totales = (hoy - inicio_op).days + 1 if inicio_op else 1

            # REGLA 1: Más de 15 días sin cerrar calefacción
            if dias_totales > 15:
                alertas_por_empresa[emp_id]['alertas_vencidos'].append(
                    f"🔸 *{row['ubicacion']}* (Lleva {dias_totales} días activo)"
                )

            # REGLA 2: Frecuencia de Consumo
            alerta_frecuencia = False
            razon_frecuencia = ""

            if dias_sin_registro >= 2:
                alerta_frecuencia = True
                razon_frecuencia = f"hace {dias_sin_registro} días"
            elif es_viernes and dias_sin_registro >= 1:
                alerta_frecuencia = True
                razon_frecuencia = "no reportado para el fin de semana"

            if alerta_frecuencia:
                alertas_por_empresa[emp_id]['alertas_frecuencia'].append(
                    f"🔹 *{row['ubicacion']}* (Último reporte: {razon_frecuencia})"
                )

        # 2. Procesar envíos empresa por empresa (Aislamiento de datos)
        for emp_id, datos in alertas_por_empresa.items():
            
            # Buscamos usuarios supervisores u operadores
            cur.execute("""
                SELECT telegram_id FROM usuarios 
                WHERE empresa_id = %s 
                  AND perfil IN ('supervisor_gas', 'operador_gas') 
                  AND telegram_id IS NOT NULL 
                  AND telegram_id != ''
            """, (emp_id,))
            usuarios_destino = cur.fetchall()

            # Construimos el encabezado estándar
            mensaje = f"📊 *REPORTE DE AUDITORÍA GLP* 📊\nEmpresa: {datos['nombre']}\n\n"

            # Evaluar si todo está bien o si hay alertas
            if not datos['alertas_frecuencia'] and not datos['alertas_vencidos']:
                mensaje += "✅ *Todo en orden.*\nNo hay granjas atrasadas ni lotes vencidos el día de hoy."
            else:
                if datos['alertas_frecuencia']:
                    mensaje += "⚠️ *Granjas sin reporte de consumo reciente:*\n"
                    mensaje += "\n".join(datos['alertas_frecuencia']) + "\n\n"
                    
                if datos['alertas_vencidos']:
                    mensaje += "🔥 *Granjas que excedieron los 15 días de calefacción:*\n"
                    mensaje += "\n".join(datos['alertas_vencidos']) + "\n\n"

                mensaje += "Por favor, contactar a los operarios de estas sedes."

            # Enviar a los destinatarios de la base de datos
            for u in usuarios_destino:
                enviar_telegram(u['telegram_id'], mensaje)
            
            # BYPASS DE SEGURIDAD: Te envía una copia directa a ti sí o sí
            enviar_telegram("5368207368", mensaje)

        cur.close()
        conn.close()
        print("✅ Auditoría finalizada correctamente.")

    except Exception as e:
        print(f"❌ Error crítico en el auditor: {e}")
        
if __name__ == "__main__":
    auditar_granjas()