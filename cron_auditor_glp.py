# -*- coding: utf-8 -*-
import MySQLdb
import requests
import time
from datetime import datetime, timedelta

# ================= TUS DATOS DE PRODUCCIÓN =================
DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
DB_USER = "baquiasoft"
DB_PASS = "Ataraxia123*/"
DB_NAME = "baquiasoft$energix_v2"
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
    print(f"🚀 INICIANDO AUDITORÍA - TANQUEOS NO REGISTRADOS: {datetime.now()}")

    try:
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        
        # REFRESCO FORZADO: Evita lecturas obsoletas de transacciones previas cacheadas
        conn.commit() 
        
        cur = conn.cursor(MySQLdb.cursors.DictCursor)

        alertas_por_empresa = {}

        # Cargar empresas activas para garantizar inicialización de estructuras por tenant
        cur.execute("SELECT DISTINCT id_empresa, empresa FROM cardex_glp WHERE estatus_lote = 'ACTIVO'")
        empresas_activas = cur.fetchall()
        for emp in empresas_activas:
            emp_id = emp['id_empresa']
            alertas_por_empresa[emp_id] = {
                'nombre': emp['empresa'],
                'alertas_pedidos': [],
                'alertas_aprobaciones': []
            }

        # -----------------------------------------------------------------
        # SITUACIÓN 1: INICIO CALEFACCIÓN CON ARRIBO CONFIRMADO SIN TANQUEO
        # -----------------------------------------------------------------
        query_tanqueos_no_registrados = """
            SELECT
                c.fecha,
                c.ubicacion,
                c.estatus_lote,
                c.codigo_pedido,
                c.operacion,
                c.id_empresa,
                c.empresa
            FROM cardex_glp c
            INNER JOIN (
                SELECT
                    ubicacion,
                    MAX(id) AS ultimo_id
                FROM cardex_glp
                WHERE estatus_lote = 'ACTIVO'
                  AND operacion = 'inicio_calefaccion'
                  AND fecha_llegada_pollitos IS NOT NULL
                  AND codigo_pedido IS NOT NULL
                  AND TRIM(codigo_pedido) <> ''
                GROUP BY ubicacion
            ) ult
                ON c.id = ult.ultimo_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM cardex_glp t
                WHERE TRIM(t.ubicacion) COLLATE utf8mb4_general_ci = TRIM(c.ubicacion) COLLATE utf8mb4_general_ci
                  AND (
                      (t.codigo_pedido IS NOT NULL AND TRIM(t.codigo_pedido) <> '' AND t.codigo_pedido COLLATE utf8mb4_general_ci = c.codigo_pedido COLLATE utf8mb4_general_ci)
                      OR (t.lote COLLATE utf8mb4_general_ci = c.lote COLLATE utf8mb4_general_ci AND t.id > c.id)
                  )
                  AND t.operacion = 'tanqueo'
            )
            ORDER BY c.ubicacion;
        """
        cur.execute(query_tanqueos_no_registrados)
        registros_situacion1 = cur.fetchall()

        for reg in registros_situacion1:
            emp_id = reg['id_empresa']
            if emp_id not in alertas_por_empresa:
                alertas_por_empresa[emp_id] = {'nombre': reg['empresa'], 'alertas_pedidos': [], 'alertas_aprobaciones': []}
            
            cod_pedido = reg['codigo_pedido'] if reg['codigo_pedido'] else 'S/N'
            fec_str = reg['fecha'].strftime('%Y-%m-%d') if reg['fecha'] else 'N/A'
            
            alertas_por_empresa[emp_id]['alertas_pedidos'].append(
                f"📍 <b>Ubicación: {reg['ubicacion']}</b>\n"
                f"   • Código Pedido: {cod_pedido}\n"
                f"   • Última Operación: {reg['operacion']}\n"
                f"   • Fecha: {fec_str}\n"
                f"   • Estado: Arribo confirmado, esperando tanqueo"
            )

        # -----------------------------------------------------------------
        # SITUACIÓN 2: PEDIDOS POR CONSUMO PENDIENTES DE TANQUEO
        # -----------------------------------------------------------------
        query_aprobaciones = """
            SELECT 
                p.cliente AS empresa, 
                p.ubicacion, 
                p.codigo_pedido, 
                p.estatus_flujo,
                e.nit AS id_empresa,
                COALESCE(p.fecha_solicitud, p.fecha_registro) AS fecha_referencia
            FROM pedidos_gas_glp p
            JOIN empresas e ON TRIM(UPPER(p.cliente)) COLLATE utf8mb4_general_ci = TRIM(UPPER(e.nombre_comercial)) COLLATE utf8mb4_general_ci
            WHERE p.estatus_lote = 'ACTIVO'
              AND p.estatus_flujo IN ('aprobado_webmaster', 'pendiente_aprobacion')
              AND NOT EXISTS (
                  SELECT 1 
                  FROM cardex_glp c 
                  WHERE TRIM(c.ubicacion) COLLATE utf8mb4_general_ci = TRIM(p.ubicacion) COLLATE utf8mb4_general_ci
                    AND (
                        (c.codigo_pedido IS NOT NULL AND TRIM(c.codigo_pedido) <> '' AND c.codigo_pedido COLLATE utf8mb4_general_ci = p.codigo_pedido COLLATE utf8mb4_general_ci)
                        OR (c.lote COLLATE utf8mb4_general_ci = p.lote COLLATE utf8mb4_general_ci AND c.fecha >= COALESCE(p.fecha_solicitud, p.fecha_registro))
                    )
                    AND c.operacion = 'tanqueo'
              )
            ORDER BY p.ubicacion;
        """
        cur.execute(query_aprobaciones)
        registros_situacion2 = cur.fetchall()

        for reg in registros_situacion2:
            emp_id = reg['id_empresa']
            if emp_id not in alertas_por_empresa:
                alertas_por_empresa[emp_id] = {'nombre': reg['empresa'], 'alertas_pedidos': [], 'alertas_aprobaciones': []}
            
            estado_ui = "Aprobado (Esperando tanqueo físico)" if reg['estatus_flujo'] == 'aprobado_webmaster' else "En Trámite (Posible tanqueo adelantado)"
            fec_ref = reg.get('fecha_referencia')
            fec_str = fec_ref.strftime('%Y-%m-%d') if fec_ref else 'Desconocida'
            
            alertas_por_empresa[emp_id]['alertas_aprobaciones'].append(
                f"📋 <b>Ubicación: {reg['ubicacion']}</b>\n"
                f"   • Código Pedido: {reg['codigo_pedido']}\n"
                f"   • Fecha Solicitud: {fec_str}\n"
                f"   • Estado: {estado_ui}"
            )

        # -----------------------------------------------------------------
        # ENSAMBLAJE Y ENVÍO DE INFORMES A TELEGRAM
        # -----------------------------------------------------------------
        for emp_id, datos in alertas_por_empresa.items():
            
            cur.execute("""
                SELECT telegram_id FROM usuarios 
                WHERE empresa_id = %s 
                  AND perfil IN ('supervisor_gas', 'operador_gas') 
                  AND telegram_id IS NOT NULL 
                  AND telegram_id != ''
            """, (emp_id,))
            usuarios_destino = cur.fetchall()
            
            destinatarios_unicos = set()
            for u in usuarios_destino:
                destinatarios_unicos.add(u['telegram_id'])
            
            # Copia de seguridad central
            destinatarios_unicos.add("5368207368")

            # Encabezado
            mensaje = f"📊 <b>REPORTE DIARIO GLP</b> 📊\nEmpresa: {datos['nombre']}\n\n"

            # Bloque 1: Situación Uno
            mensaje += "🚨 <b>TANQUEOS PENDIENTES (INICIO LOTE):</b>\n"
            if not datos.get('alertas_pedidos'):
                mensaje += "✅ <i>Todo en orden. No hay operaciones omitidas.</i>\n\n"
            else:
                mensaje += "\n\n".join(datos['alertas_pedidos']) + "\n\n"

            # Bloque 2: Situación Dos
            mensaje += "⚠️ <b>TANQUEOS PENDIENTES (PEDIDOS DE CONSUMO):</b>\n"
            if not datos.get('alertas_aprobaciones'):
                mensaje += "✅ <i>Todo en orden. No hay pedidos de consumo sin tanquear.</i>"
            else:
                mensaje += "\n\n".join(datos['alertas_aprobaciones'])

            # Enviar a la red de usuarios
            for chat_id in destinatarios_unicos:
                enviar_telegram(chat_id, mensaje)
                
        cur.close()
        conn.close()
        print("✅ Auditoría de tanqueos no registrados finalizada correctamente.")

    except Exception as e:
        print(f"❌ Error crítico en el auditor: {e}")
        
if __name__ == "__main__":
    auditar_granjas()