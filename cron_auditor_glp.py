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
    # CAMBIO: Usamos HTML para que no colapse si una granja tiene caracteres especiales (_)
    data = {"chat_id": chat_id, "text": mensaje, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, data=data, timeout=10)
        # Trazabilidad de Red
        if resp.status_code != 200:
            print(f"⚠️ Telegram rechazó el mensaje a {chat_id}: {resp.text}")
    except Exception as e:
        print(f"❌ Error de red enviando a {chat_id}: {e}")

def auditar_granjas():
    print(f"Iniciando auditoría GLP: {datetime.now()}")
    hoy = datetime.now().date()
    es_viernes = datetime.now().weekday() == 4 # 0=Lunes, 4=Viernes

    try:
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        cur = conn.cursor(MySQLdb.cursors.DictCursor)

        # 1. CONSULTA OPTIMIZADA: Lotes Activos (Restaurado COALESCE para 15 días exactos)
        query_lotes = """
            SELECT 
                id_empresa, 
                empresa, 
                ubicacion, 
                lote, 
                MAX(fecha) as ultima_operacion,
                COALESCE(MIN(fecha_llegada_pollitos), MIN(fecha)) as fecha_inicio
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
                    'alertas_vencidos': [],
                    'alertas_pedidos': []
                }

            ultima_op = row['ultima_operacion']
            inicio_op = row['fecha_inicio']
            
            dias_sin_registro = (hoy - ultima_op).days if ultima_op else 0
            dias_totales = (hoy - inicio_op).days + 1 if inicio_op else 1

            # REGLA 1: Más de 15 días sin cerrar calefacción
            if dias_totales > 15:
                alertas_por_empresa[emp_id]['alertas_vencidos'].append(
                    f"🔸 <b>{row['ubicacion']}</b> (Lleva {dias_totales} días activo)"
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
                    f"🔹 <b>{row['ubicacion']}</b> (Último reporte: {razon_frecuencia})"
                )

        # 2. CONSULTA BLINDADA TOTAL: Pedidos aprobados sin registrar tanqueo (>= 3 días) para lotes ACTIVOS
        query_pedidos = """
            SELECT 
                p.cliente AS empresa, 
                e.nit AS id_empresa, 
                p.ubicacion, 
                p.codigo_pedido, 
                p.fecha_registro,
                DATEDIFF(CURDATE(), DATE(p.fecha_registro)) AS dias_retraso
            FROM pedidos_gas_glp p
            JOIN empresas e ON TRIM(UPPER(p.cliente)) COLLATE utf8mb4_general_ci = TRIM(UPPER(e.nombre_comercial)) COLLATE utf8mb4_general_ci
            WHERE p.estatus_flujo IN ('aprobado_webmaster', 'enviado_auto')
              AND DATEDIFF(CURDATE(), DATE(p.fecha_registro)) >= 3
              AND EXISTS (
                  SELECT 1 FROM cardex_glp c 
                  WHERE c.lote COLLATE utf8mb4_general_ci = p.lote COLLATE utf8mb4_general_ci 
                    AND c.estatus_lote = 'ACTIVO'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM cardex_glp c2 
                  WHERE c2.codigo_pedido COLLATE utf8mb4_general_ci = p.codigo_pedido COLLATE utf8mb4_general_ci 
                    AND c2.operacion = 'tanqueo'
              )
        """
        cur.execute(query_pedidos)
        pedidos_huerfanos = cur.fetchall()

        for p in pedidos_huerfanos:
            emp_id = p['id_empresa']
            if emp_id not in alertas_por_empresa:
                alertas_por_empresa[emp_id] = {
                    'nombre': p['empresa'],
                    'alertas_frecuencia': [],
                    'alertas_vencidos': [],
                    'alertas_pedidos': []
                }
            alertas_por_empresa[emp_id]['alertas_pedidos'].append(
                f"⏳ <b>{p['ubicacion']}</b> - Pedido: {p['codigo_pedido']} (Aprobado hace {p['dias_retraso']} días, sin registrar)"
            )

        # 3. Procesar envíos empresa por empresa (Aislamiento de datos)
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

            # Construimos el encabezado estándar en formato HTML
            mensaje = f"📊 <b>REPORTE DE AUDITORÍA GLP</b> 📊\nEmpresa: {datos['nombre']}\n\n"

            # Evaluar si todo está bien o si hay alertas
            if not datos['alertas_frecuencia'] and not datos['alertas_vencidos'] and not datos['alertas_pedidos']:
                mensaje += "✅ <b>Todo en orden.</b>\nNo hay granjas atrasadas, lotes vencidos ni pedidos sin registrar el día de hoy."
            else:
                if datos['alertas_frecuencia']:
                    mensaje += "⚠️ <b>Granjas sin reporte de consumo reciente:</b>\n"
                    mensaje += "\n".join(datos['alertas_frecuencia']) + "\n\n"
                    
                if datos['alertas_vencidos']:
                    mensaje += "🔥 <b>Granjas que excedieron los 15 días de calefacción:</b>\n"
                    mensaje += "\n".join(datos['alertas_vencidos']) + "\n\n"

                if datos['alertas_pedidos']:
                    mensaje += "⏳ <b>Pedidos de gas aprobados sin registrar:</b>\n"
                    mensaje += "\n".join(datos['alertas_pedidos']) + "\n\n"

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