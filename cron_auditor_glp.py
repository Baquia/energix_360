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

        # 1. Buscamos TODOS los lotes activos y calculamos sus fechas clave
        query_lotes = """
            SELECT 
                empresa_id, 
                empresa, 
                ubicacion, 
                lote, 
                MAX(fecha) as ultima_operacion,
                (SELECT MIN(fecha) FROM cardex_glp c2 WHERE c2.lote = c1.lote) as fecha_inicio
            FROM cardex_glp c1
            WHERE estatus_lote = 'ACTIVO'
            GROUP BY empresa_id, empresa, ubicacion, lote
        """
        cur.execute(query_lotes)
        lotes_activos = cur.fetchall()

        # Diccionario para agrupar alertas por empresa: {empresa_id: {nombre: '', alertas_frecuencia: [], alertas_vencidos: []}}
        alertas_por_empresa = {}

        for row in lotes_activos:
            emp_id = row['empresa_id']
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

            # REGLA 2: Frecuencia de Consumo (Pasaron 2 días, o es viernes y no han reportado hoy/ayer)
            # Si es viernes, exigimos que el último registro sea de máximo hace 1 día (jueves o viernes)
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
            
            # Si esta empresa no tiene ninguna alerta, la saltamos
            if not datos['alertas_frecuencia'] and not datos['alertas_vencidos']:
                continue

            # Construir el mensaje específico para esta empresa
            mensaje = f"📊 *REPORTE DE AUDITORÍA GLP* 📊\nEmpresa: {datos['nombre']}\n\n"
            
            if datos['alertas_frecuencia']:
                mensaje += "⚠️ *Granjas sin reporte de consumo reciente:*\n"
                mensaje += "\n".join(datos['alertas_frecuencia']) + "\n\n"
                
            if datos['alertas_vencidos']:
                mensaje += "🔥 *Granjas que excedieron los 15 días de calefacción:*\n"
                mensaje += "\n".join(datos['alertas_vencidos']) + "\n\n"

            mensaje += "Por favor, contactar a los operarios de estas sedes."

            # Buscar a los supervisores/webmasters SOLO de esta empresa
            cur.execute("""
                SELECT telegram_id FROM usuarios 
                WHERE empresa_id = %s AND telegram_id IS NOT NULL AND telegram_id != ''
            """, (emp_id,))
            usuarios_destino = cur.fetchall()

            for u in usuarios_destino:
                enviar_telegram(u['telegram_id'], mensaje)

        cur.close()
        conn.close()
        print("✅ Auditoría finalizada correctamente.")

    except Exception as e:
        print(f"❌ Error crítico en el auditor: {e}")

if __name__ == "__main__":
    auditar_granjas()