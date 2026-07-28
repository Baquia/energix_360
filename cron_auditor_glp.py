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
    hoy = datetime.now().date()

    try:
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        
        # ⚠️ REFRESCO FORZADO: Rompe cualquier "Lectura Fantasma" de transacciones previas cacheadas
        conn.commit() 
        
        cur = conn.cursor(MySQLdb.cursors.DictCursor)

        # Diccionario para agrupar alertas exclusivamente por empresa
        alertas_por_empresa = {}

        # Cargar todas las empresas con lotes ACTIVOS para garantizar que siempre se construya e informe
        cur.execute("SELECT DISTINCT id_empresa, empresa FROM cardex_glp WHERE estatus_lote = 'ACTIVO'")
        empresas_activas = cur.fetchall()
        for emp in empresas_activas:
            emp_id = emp['id_empresa']
            alertas_por_empresa[emp_id] = {
                'nombre': emp['empresa'],
                'alertas_pedidos': []
            }

        # -----------------------------------------------------------------
        # PASO 1: Obtener solicitudes de gas en lotes ACTIVOS
        # -----------------------------------------------------------------
        query_solicitudes = """
            SELECT id, id_empresa, empresa, ubicacion, lote, operacion, codigo_pedido, fecha
            FROM cardex_glp
            WHERE estatus_lote = 'ACTIVO'
              AND operacion IN ('inicio_calefaccion', 'consumo')
              AND codigo_pedido IS NOT NULL
              AND TRIM(codigo_pedido) <> ''
            ORDER BY fecha DESC, id DESC;
        """
        cur.execute(query_solicitudes)
        solicitudes = cur.fetchall()

        # -----------------------------------------------------------------
        # PASO 2: Filtrar la última solicitud por sede y consultar tanqueo
        # -----------------------------------------------------------------
        ubicaciones_procesadas = set()

        for sol in solicitudes:
            clave_ubicacion = (sol['id_empresa'], sol['ubicacion'])
            
            # Garantizar evaluacion exclusiva de la última solicitud por sede (rn = 1)
            if clave_ubicacion in ubicaciones_procesadas:
                continue
            
            ubicaciones_procesadas.add(clave_ubicacion)

            # Consulta individual para verificar si existe un tanqueo registrado posteriormente
            query_tanqueo = """
                SELECT COUNT(*) AS total
                FROM cardex_glp
                WHERE operacion = 'tanqueo'
                  AND id_empresa = %s
                  AND ubicacion = %s
                  AND lote = %s
                  AND (codigo_pedido = %s OR id > %s);
            """
            cur.execute(query_tanqueo, (
                sol['id_empresa'],
                sol['ubicacion'],
                sol['lote'],
                sol['codigo_pedido'],
                sol['id']
            ))
            res_tanqueo = cur.fetchone()
            total_tanqueos = res_tanqueo['total'] if res_tanqueo else 0

            # -------------------------------------------------------------
            # PASO 3: Evaluar retraso si NO existe registro de tanqueo
            # -------------------------------------------------------------
            if total_tanqueos == 0:
                fecha_solicitud = sol['fecha']
                dias_retraso = (hoy - fecha_solicitud).days if fecha_solicitud else 0

                if dias_retraso > 3:
                    emp_id = sol['id_empresa']
                    if emp_id not in alertas_por_empresa:
                        alertas_por_empresa[emp_id] = {
                            'nombre': sol['empresa'],
                            'alertas_pedidos': []
                        }
                    alertas_por_empresa[emp_id]['alertas_pedidos'].append(
                        f"⏳ <b>{sol['ubicacion']}</b> - Pedido: {sol['codigo_pedido']} (Atraso: {dias_retraso} días. Estado: Pendiente de Registro)"
                    )

        # -----------------------------------------------------------------
        # PASO 4: Procesar envíos a Telegram por empresa
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