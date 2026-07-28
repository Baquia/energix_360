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
    # MARCA DE AGUA V10 PARA AUDITAR CACHÉ DE PYTHONANYWHERE
    print(f"🚀 INICIANDO AUDITORÍA V10: {datetime.now()}")

    try:
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        
        # ⚠️ REFRESCO FORZADO: Rompe cualquier "Lectura Fantasma" de transacciones previas cacheadas
        conn.commit() 
        
        cur = conn.cursor(MySQLdb.cursors.DictCursor)

        # Diccionario para agrupar alertas exclusivamente por empresa
        alertas_por_empresa = {}

        # 1. Cargar todas las empresas que tienen lotes ACTIVOS para garantizar envío de Telegram
        cur.execute("SELECT DISTINCT id_empresa, empresa FROM cardex_glp WHERE estatus_lote = 'ACTIVO'")
        empresas_activas = cur.fetchall()
        for emp in empresas_activas:
            emp_id = emp['id_empresa']
            alertas_por_empresa[emp_id] = {
                'nombre': emp['empresa'],
                'alertas_pedidos': []
            }

        # 2. CONSULTA ÚNICA V10: Algoritmo de rastreo físico basado 100% en cardex_glp
        query_pedidos = """
        WITH OperacionesConPedido AS (
            SELECT
                c.id_empresa,
                c.empresa,
                c.ubicacion,
                c.lote,
                c.operacion,
                c.codigo_pedido,
                c.fecha,
                c.id,
                ROW_NUMBER() OVER (
                    PARTITION BY c.id_empresa, c.ubicacion
                    ORDER BY c.fecha DESC, c.id DESC
                ) AS rn
            FROM cardex_glp c
            WHERE c.estatus_lote = 'ACTIVO'
              AND c.operacion IN ('inicio_calefaccion', 'consumo')
              AND c.codigo_pedido IS NOT NULL
              AND TRIM(c.codigo_pedido) <> ''
        )
        SELECT
            o.id_empresa,
            o.empresa,
            o.ubicacion,
            o.operacion,
            o.codigo_pedido,
            o.fecha,
            'Pendiente de Registro' AS estatus_flujo,
            DATEDIFF(CURDATE(), o.fecha) AS dias_retraso
        FROM OperacionesConPedido o
        WHERE o.rn = 1
          AND DATEDIFF(CURDATE(), o.fecha) > 3
          AND NOT EXISTS (
              SELECT 1
              FROM cardex_glp t
              WHERE t.id_empresa = o.id_empresa
                AND t.ubicacion = o.ubicacion
                AND t.lote = o.lote
                AND t.operacion = 'tanqueo'
                AND t.id > o.id
          )
        ORDER BY dias_retraso DESC, o.ubicacion;
        """
        cur.execute(query_pedidos)
        pedidos_huerfanos = cur.fetchall()

        for p in pedidos_huerfanos:
            emp_id = p['id_empresa']
            if emp_id not in alertas_por_empresa:
                alertas_por_empresa[emp_id] = {
                    'nombre': p['empresa'],
                    'alertas_pedidos': []
                }
            alertas_por_empresa[emp_id]['alertas_pedidos'].append(
                f"⏳ <b>{p['ubicacion']}</b> - Pedido: {p['codigo_pedido']} (Atraso: {p['dias_retraso']} días. Estado: {p['estatus_flujo']})"
            )

        # 3. Procesar envíos a Telegram por empresa
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

            if not datos['alertas_pedidos']:
                mensaje += "✅ <b>Todo en orden.</b>\nNo hay pedidos pendientes de registrar tanqueo con más de 3 días de atraso el día de hoy."
            else:
                mensaje += "🔍 <b>AUDITORÍA V10 - Pedidos de gas pendientes de registrar tanqueo:</b>\n"
                mensaje += "\n".join(datos['alertas_pedidos']) + "\n\n"
                mensaje += "Por favor, contactar a los operarios de estas sedes."

            # Enviar a la colección de destinatarios únicos
            for chat_id in destinatarios_unicos:
                enviar_telegram(chat_id, mensaje)

        cur.close()
        conn.close()
        print("✅ Auditoría V10 finalizada correctamente.")

    except Exception as e:
        print(f"❌ Error crítico en el auditor V10: {e}")
        
if __name__ == "__main__":
    auditar_granjas()