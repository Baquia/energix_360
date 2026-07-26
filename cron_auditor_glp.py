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

        # 1. CONSULTA OPTIMIZADA: Lotes Activos
        query_lotes = """
            SELECT 
                c.id_empresa, 
                c.empresa, 
                c.ubicacion, 
                c.lote, 
                MAX(c.fecha) as ultima_operacion,
                COALESCE(MIN(c.fecha_llegada_pollitos), MIN(c.fecha)) as fecha_inicio
            FROM cardex_glp c
            LEFT JOIN cardex_glp f ON c.lote = f.lote AND c.id_empresa = f.id_empresa AND f.operacion = 'finalizar_calefaccion'
            WHERE f.id IS NULL
            GROUP BY c.id_empresa, c.empresa, c.ubicacion, c.lote
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
                    f"🔹 *{row['ubicacion']}* (Sin actividad registrada: {razon_frecuencia})"
                )

        # 2. CONSULTA NUEVA: Pedidos generados sin registrar tanqueo (>= 3 días)
        query_pedidos = """
            SELECT 
                p.cliente AS empresa, 
                e.nit AS id_empresa, 
                p.ubicacion, 
                p.codigo_pedido, 
                p.fecha_registro,
                DATEDIFF(CURDATE(), DATE(p.fecha_registro)) AS dias_retraso
            FROM pedidos_gas_glp p
            JOIN empresas e ON TRIM(UPPER(p.cliente)) = TRIM(UPPER(e.nombre_comercial))
            WHERE p.estatus = 'generado' 
              AND p.estatus_flujo NOT IN ('tanqueo_registrado', 'legalizado_extemporaneo', 'anulado_sin_evidencia')
              AND DATEDIFF(CURDATE(), DATE(p.fecha_registro)) >= 3
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
                f"⏳ *{p['ubicacion']}* - Pedido: {p['codigo_pedido']} (Generado hace {p['dias_retraso']} días)"
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

            # Construimos el encabezado estándar
            mensaje = f"📊 *REPORTE DE AUDITORÍA GLP* 📊\nEmpresa: {datos['nombre']}\n\n"

            # Evaluar si todo está bien o si hay alertas
            if not datos['alertas_frecuencia'] and not datos['alertas_vencidos'] and not datos['alertas_pedidos']:
                mensaje += "✅ *Todo en orden.*\nNo hay granjas atrasadas, lotes vencidos ni pedidos sin registrar el día de hoy."
            else:
                if datos['alertas_frecuencia']:
                    mensaje += "⚠️ *Granjas sin actividad registrada (consumo/tanqueo):*\n"
                    mensaje += "\n".join(datos['alertas_frecuencia']) + "\n\n"
                    
                if datos['alertas_vencidos']:
                    mensaje += "🔥 *Granjas que excedieron los 15 días de calefacción:*\n"
                    mensaje += "\n".join(datos['alertas_vencidos']) + "\n\n"
                    
                if datos['alertas_pedidos']:
                    mensaje += "⏳ *Pedidos de gas sin registrar (Más de 3 días):*\n"
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