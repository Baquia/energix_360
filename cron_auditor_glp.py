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
                'alertas_pedidos': []
            }

        # -----------------------------------------------------------------
        # CONSULTA SQL ÚNICA SOLICITADA (Con inclusión de id_empresa/empresa para multitenancy)
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
                  AND operacion IN ('inicio_calefaccion', 'consumo')
                GROUP BY ubicacion
            ) ult
                ON c.id = ult.ultimo_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM cardex_glp t
                WHERE t.codigo_pedido = c.codigo_pedido
                  AND t.ubicacion = c.ubicacion
                  AND t.operacion = 'registrar_tanqueo'
                  AND t.id > c.id
            )
            ORDER BY c.ubicacion;
        """
        cur.execute(query_tanqueos_no_registrados)
        registros = cur.fetchall()

        # -----------------------------------------------------------------
        # PROCESAMIENTO Y DISCRIMINACIÓN POR UBICACIÓN
        # -----------------------------------------------------------------
        for reg in registros:
            emp_id = reg['id_empresa']
            if emp_id not in alertas_por_empresa:
                alertas_por_empresa[emp_id] = {
                    'nombre': reg['empresa'],
                    'alertas_pedidos': []
                }
            
            cod_pedido = reg['codigo_pedido'] if reg['codigo_pedido'] else 'S/N'
            fec_str = reg['fecha'].strftime('%Y-%m-%d') if reg['fecha'] else 'N/A'
            
            alertas_por_empresa[emp_id]['alertas_pedidos'].append(
                f"📍 <b>Ubicación: {reg['ubicacion']}</b>\n"
                f"   • Código Pedido: {cod_pedido}\n"
                f"   • Última Operación: {reg['operacion']}\n"
                f"   • Fecha: {fec_str}\n"
                f"   • Estado Lote: {reg['estatus_lote']}"
            )

        # -----------------------------------------------------------------
        # ENVÍO DE INFORMES A TELEGRAM
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
            
            # Copia de seguridad
            destinatarios_unicos.add("5368207368")

            # Encabezado con el nombre exacto del informe
            mensaje = f"📊 <b>TANQUEOS NO REGISTRADOS</b> 📊\nEmpresa: {datos['nombre']}\n\n"

            if not datos['alertas_pedidos']:
                mensaje += "✅ <b>Todo en orden.</b>\nNo hay sedes con tanqueos pendientes de registro para sus lotes activos."
            else:
                mensaje += "🔍 <b>Reporte discriminado por ubicación:</b>\n\n"
                mensaje += "\n\n".join(datos['alertas_pedidos']) + "\n\n"
                mensaje += "Por favor, contactar a los operarios de estas sedes."

            for chat_id in destinatarios_unicos:
                enviar_telegram(chat_id, mensaje)

        cur.close()
        conn.close()
        print("✅ Auditoría de tanqueos no registrados finalizada correctamente.")

    except Exception as e:
        print(f"❌ Error crítico en el auditor: {e}")
        
if __name__ == "__main__":
    auditar_granjas()