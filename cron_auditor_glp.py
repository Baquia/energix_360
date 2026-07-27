# -*- coding: utf-8 -*-
import MySQLdb
import requests
import time
from datetime import datetime, timedelta

# ================= TUS DATOS DE PRODUCCIÓN =================
DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
DB_USER = "baquiasoft"
DB_PASS = "Ataraxia123*/"
DB_NAME = "baquiasoft$energix_360"
TOKEN_TELEGRAM = "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"
# ==========================================================

def enviar_telegram(chat_id, mensaje, max_reintentos=3):
    url = f"https://api.telegram.org/bot{TOKEN_TELEGRAM}/sendMessage"
    data = {"chat_id": chat_id, "text": mensaje, "parse_mode": "HTML"}
    
    # Sistema de Reintentos Automáticos (Retry-Backoff) para fallos del Proxy 503
    for intento in range(1, max_reintentos + 1):
        try:
            resp = requests.post(url, data=data, timeout=15)
            if resp.status_code == 200:
                return True
            else:
                print(f"⚠️ [Intento {intento}] Telegram rechazó el mensaje a {chat_id}: {resp.text}")
        except Exception as e:
            print(f"❌ [Intento {intento}] Error de red enviando a {chat_id}: {e}")
        
        if intento < max_reintentos:
            time.sleep(5)
            
    print(f"💀 Fracaso definitivo enviando a {chat_id} tras {max_reintentos} intentos por inestabilidad de red.")
    return False

def auditar_granjas():
    # MARCA DE AGUA V9 PARA AUDITAR CACHÉ DE PYTHONANYWHERE
    print(f"🚀 INICIANDO AUDITORÍA V9: {datetime.now()}")
    hoy = datetime.now().date()
    es_viernes = datetime.now().weekday() == 4 # 0=Lunes, 4=Viernes

    try:
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        
        # ⚠️ REFRESCO FORZADO: Rompe cualquier "Lectura Fantasma" de transacciones previas cacheadas
        conn.commit() 
        
        cur = conn.cursor(MySQLdb.cursors.DictCursor)

        # 1. CONSULTA OPTIMIZADA Y BLINDADA: Filtra estrictamente por el último estado histórico del LOTE
        query_lotes = """
            SELECT 
                c.id_empresa, 
                c.empresa, 
                c.ubicacion, 
                c.lote, 
                MAX(c.fecha) as ultima_operacion,
                COALESCE(MIN(c.fecha_llegada_pollitos), MIN(c.fecha)) as fecha_inicio
            FROM cardex_glp c
            INNER JOIN (
                SELECT t1.lote
                FROM cardex_glp t1
                INNER JOIN (
                    SELECT lote, MAX(id) as max_id
                    FROM cardex_glp
                    GROUP BY lote
                ) t2 ON t1.id = t2.max_id
                WHERE t1.estatus_lote = 'ACTIVO'
            ) activos ON c.lote = activos.lote
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

        # 2. CONSULTA V9: Regla estricta de la "Realidad Física" de la granja (cardex -> pedidos)
        query_pedidos = """
            SELECT 
                c.empresa, 
                c.id_empresa, 
                c.ubicacion, 
                c.codigo_pedido, 
                p.fecha_registro,
                p.estatus_flujo,
                DATEDIFF(CURDATE(), DATE(p.fecha_registro)) AS dias_retraso
            FROM cardex_glp c
            INNER JOIN (
                SELECT ubicacion, MAX(id) as max_id
                FROM cardex_glp
                GROUP BY ubicacion
            ) ultimos ON c.id = ultimos.max_id
            JOIN pedidos_gas_glp p 
              ON c.codigo_pedido COLLATE utf8mb4_general_ci = p.codigo_pedido COLLATE utf8mb4_general_ci
            WHERE c.estatus_lote = 'ACTIVO'
              AND c.operacion IN ('inicio_calefaccion', 'consumo')
              AND c.codigo_pedido IS NOT NULL 
              AND c.codigo_pedido != ''
              AND DATEDIFF(CURDATE(), DATE(p.fecha_registro)) >= 3
              AND p.estatus_flujo NOT IN ('tanqueo_registrado', 'anulado_sin_evidencia', 'legalizado_extemporaneo')
              AND p.estatus NOT IN ('rechazado', 'cancelado', 'anulado')
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
                f"⏳ <b>{p['ubicacion']}</b> - Pedido: {p['codigo_pedido']} (Atraso: {p['dias_retraso']} días. Estado: {p['estatus_flujo']})"
            )

        # 3. Procesar envíos empresa por empresa
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
            
            # ⚠️ ANTI-DUPLICADOS: Usamos un Set para almacenar IDs únicos de envío
            destinatarios_unicos = set()
            for u in usuarios_destino:
                destinatarios_unicos.add(u['telegram_id'])
            
            # BYPASS DE SEGURIDAD: Te enviamos una copia
            destinatarios_unicos.add("5368207368")

            # Encabezado estándar en formato HTML
            mensaje = f"📊 <b>REPORTE DE AUDITORÍA GLP</b> 📊\nEmpresa: {datos['nombre']}\n\n"

            if not datos['alertas_frecuencia'] and not datos['alertas_vencidos'] and not datos['alertas_pedidos']:
                mensaje += "✅ <b>Todo en orden.</b>\nNo hay granjas atrasadas, lotes vencidos ni pedidos en tránsito el día de hoy."
            else:
                if datos['alertas_frecuencia']:
                    mensaje += "⚠️ <b>Granjas sin reporte de consumo reciente:</b>\n"
                    mensaje += "\n".join(datos['alertas_frecuencia']) + "\n\n"
                    
                if datos['alertas_vencidos']:
                    mensaje += "🔥 <b>Granjas que excedieron los 15 días de calefacción:</b>\n"
                    mensaje += "\n".join(datos['alertas_vencidos']) + "\n\n"

                if datos['alertas_pedidos']:
                    # TÍTULO V9 RASTREADOR:
                    mensaje += "🔍 <b>AUDITORÍA V9 - Pedidos de gas en tránsito:</b>\n"
                    mensaje += "\n".join(datos['alertas_pedidos']) + "\n\n"

                mensaje += "Por favor, contactar a los operarios de estas sedes."

            # Enviar a la colección de destinatarios únicos
            for chat_id in destinatarios_unicos:
                enviar_telegram(chat_id, mensaje)

        cur.close()
        conn.close()
        print("✅ Auditoría V9 finalizada correctamente.")

    except Exception as e:
        print(f"❌ Error crítico en el auditor V9: {e}")
        
if __name__ == "__main__":
    auditar_granjas()