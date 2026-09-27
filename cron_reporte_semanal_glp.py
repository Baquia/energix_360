# -*- coding: utf-8 -*-
import sys
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import make_msgid, formatdate
from datetime import datetime, timedelta
import MySQLdb

# 1. Extraer dinámicamente las variables de correo desde wsgi.py (Arquitectura Limpia)
wsgi_path = '/var/www/baquiasoft_pythonanywhere_com_wsgi.py'
if os.path.exists(wsgi_path):
    try:
        with open(wsgi_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip().startswith('os.environ'):
                    exec(line.strip())
    except Exception as e:
        print(f"⚠️ Aviso al leer wsgi.py: {e}")

# 2. Credenciales de Producción (Conexión Nativa sin Flask)
DB_HOST = "baquiasoft.mysql.pythonanywhere-services.com"
DB_USER = "baquiasoft"
DB_PASS = "Ataraxia123*/"
DB_NAME = "baquiasoft$energix_v2"

def generar_reporte_semanal():
    print(f"🚀 INICIANDO REPORTE SEMANAL GLP: {datetime.now()}")
    
    # Heredar credenciales del entorno procesado
    EMAIL_HOST = os.environ.get('EMAIL_HOST', 'smtp.gmail.com')
    EMAIL_PORT = int(os.environ.get('EMAIL_PORT', 587))
    EMAIL_USER = os.environ.get('EMAIL_USER')
    EMAIL_PASS = os.environ.get('EMAIL_PASS')
    EMAIL_FROM = os.environ.get('EMAIL_FROM', f"BQA-ONE - Gestión Energética <{EMAIL_USER}>")
    EMAIL_ADMIN = EMAIL_USER
    
    if not EMAIL_USER or not EMAIL_PASS:
        print("❌ Error: No se encontraron credenciales SMTP en el entorno.")
        return

    # Ventana de Tiempo (Día 1 del mes hasta ayer)
    hoy = datetime.now()
    fecha_fin = (hoy - timedelta(days=1)).date()
    fecha_inicio = hoy.replace(day=1).date()
    
    try:
        # Conexión nativa a prueba de fallos de entorno
        conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
        conn.commit() # Saneamiento de caché
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        
        cur.execute("SELECT DISTINCT id_empresa, empresa FROM cardex_glp WHERE estatus_lote = 'ACTIVO'")
        empresas = cur.fetchall()
        
        for emp in empresas:
            emp_id = emp['id_empresa']
            emp_nombre = emp['empresa']
            
            cur.execute("""
                SELECT email 
                FROM usuarios 
                WHERE empresa_id = %s 
                  AND perfil = 'supervisor_gas' 
                  AND email IS NOT NULL 
                  AND email != ''
            """, (emp_id,))
            destinatarios = [row['email'] for row in cur.fetchall()]
            
            # A) Ranking de consumo
            cur.execute("""
                SELECT ubicacion, MAX(kg_pollito) as max_kg_pollito
                FROM cardex_glp
                WHERE id_empresa = %s AND estatus_lote = 'ACTIVO' AND fecha BETWEEN %s AND %s
                GROUP BY ubicacion
                ORDER BY max_kg_pollito DESC
            """, (emp_id, fecha_inicio, fecha_fin))
            ranking = cur.fetchall()
            
            # B) Costos Acumulados
            cur.execute("""
                SELECT ubicacion, SUM(COALESCE(precio_total, 0)) as costo_total
                FROM cardex_glp
                WHERE id_empresa = %s AND estatus_lote = 'ACTIVO' AND fecha BETWEEN %s AND %s
                GROUP BY ubicacion
                ORDER BY costo_total DESC
            """, (emp_id, fecha_inicio, fecha_fin))
            costos = cur.fetchall()
            total_costo_empresa = sum([(c['costo_total'] or 0) for c in costos])
            
            # C) Auditoría Multi-Tenant (Estricto WHERE id_empresa)
            cur.execute("""
                SELECT c.fecha, c.ubicacion, c.estatus_lote, c.codigo_pedido, c.operacion
                FROM cardex_glp c
                INNER JOIN (
                    SELECT ubicacion, MAX(id) AS ultimo_id
                    FROM cardex_glp
                    WHERE id_empresa = %s AND estatus_lote = 'ACTIVO' 
                      AND operacion IN ('inicio_calefaccion', 'consumo')
                      AND codigo_pedido IS NOT NULL AND TRIM(codigo_pedido) <> ''
                      AND fecha BETWEEN %s AND %s
                    GROUP BY ubicacion
                ) ult ON c.id = ult.ultimo_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM cardex_glp t
                    WHERE t.codigo_pedido = c.codigo_pedido 
                      AND t.ubicacion = c.ubicacion 
                      AND t.operacion = 'tanqueo'
                      AND t.id_empresa = %s
                )
                ORDER BY c.ubicacion;
            """, (emp_id, fecha_inicio, fecha_fin, emp_id))
            auditoria = cur.fetchall()
            
            if destinatarios or ranking or costos or auditoria:
                _enviar_correo(
                    emp_nombre, fecha_inicio, fecha_fin, ranking, costos, 
                    total_costo_empresa, auditoria, destinatarios,
                    EMAIL_HOST, EMAIL_PORT, EMAIL_USER, EMAIL_PASS, EMAIL_FROM, EMAIL_ADMIN
                )

        cur.close()
        conn.close()
        print("✅ Ejecución de reportes semanales finalizada.")

    except Exception as e:
        print(f"❌ Error crítico en el reporte semanal: {e}")

def _enviar_correo(empresa, f_ini, f_fin, ranking, costos, total_costo, auditoria, destinatarios, e_host, e_port, e_user, e_pass, e_from, e_admin):
    html_ranking = ""
    for idx, r in enumerate(ranking, 1):
        valor_kg = r['max_kg_pollito'] or 0
        html_ranking += f"<tr><td style='padding:10px; border-bottom:1px solid #eee;'>#{idx}</td><td style='padding:10px; border-bottom:1px solid #eee;'>{r['ubicacion']}</td><td style='padding:10px; border-bottom:1px solid #eee; font-weight:bold; color:#015249;'>{valor_kg:.4f}</td></tr>"
    if not html_ranking: html_ranking = "<tr><td colspan='3' style='padding:15px; text-align:center; color:#666;'>No hay datos de consumo en este periodo.</td></tr>"

    html_costos = ""
    for c in costos:
        valor_costo = c['costo_total'] or 0
        html_costos += f"<tr><td style='padding:10px; border-bottom:1px solid #eee;'>{c['ubicacion']}</td><td style='padding:10px; border-bottom:1px solid #eee; text-align:right;'>${valor_costo:,.0f}</td></tr>"
    if not html_costos: html_costos = "<tr><td colspan='2' style='padding:15px; text-align:center; color:#666;'>No hay registros de costos.</td></tr>"

    html_audit = ""
    for a in auditoria:
        html_audit += f"<tr><td style='padding:10px; border-bottom:1px solid #eee;'>{a['fecha']}</td><td style='padding:10px; border-bottom:1px solid #eee;'>{a['ubicacion']}</td><td style='padding:10px; border-bottom:1px solid #eee; font-weight:bold; color:#d9534f;'>{a['codigo_pedido']}</td><td style='padding:10px; border-bottom:1px solid #eee; color:#f59e0b;'>{a['operacion']}</td></tr>"
    if not html_audit: html_audit = "<tr><td colspan='4' style='padding:15px; text-align:center; color:#10b981; font-weight:bold;'>✅ Todos los tanqueos solicitados están registrados al día.</td></tr>"

    cuerpo_html = f"""
    <!DOCTYPE html>
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; background-color: #f4f7f6; padding: 20px; margin: 0;">
        <div style="max-width: 700px; margin: 0 auto; background: white; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); overflow: hidden;">
            <div style="background-color: #015249; color: white; padding: 30px; text-align: center;">
                <h2 style="margin: 0; font-size: 24px; font-weight: 700;">Reporte Operativo Semanal GLP</h2>
                <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 15px;">{empresa} | {f_ini.strftime('%d/%m/%Y')} al {f_fin.strftime('%d/%m/%Y')}</p>
            </div>
            
            <div style="padding: 30px;">
                <h3 style="color: #015249; border-bottom: 2px solid #015249; padding-bottom: 8px; margin-bottom: 5px;">A. Ranking de Consumo</h3>
                <p style="font-size: 13px; color: #666; margin-bottom: 15px;">Granjas ordenadas de mayor a menor consumo (kg/ave) en lotes activos durante el periodo.</p>
                <table style="width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 35px;">
                    <tr style="background-color: #f8f9fa; text-align: left; border-bottom: 2px solid #ddd;">
                        <th style="padding: 12px;">Posición</th><th style="padding: 12px;">Granja</th><th style="padding: 12px;">Kg / Ave</th>
                    </tr>
                    {html_ranking}
                </table>

                <h3 style="color: #015249; border-bottom: 2px solid #015249; padding-bottom: 8px; margin-bottom: 5px;">B. Costos e Inversión</h3>
                <p style="font-size: 13px; color: #666; margin-bottom: 15px;">Costos acumulados por tanqueos y consumos liquidados en el periodo.</p>
                <table style="width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 35px;">
                    <tr style="background-color: #f8f9fa; text-align: left; border-bottom: 2px solid #ddd;">
                        <th style="padding: 12px;">Granja</th><th style="padding: 12px; text-align: right;">Costo ($)</th>
                    </tr>
                    {html_costos}
                    <tr style="background-color: #e8f5e9; border-top: 2px solid #015249;">
                        <td style="padding: 15px; font-weight: bold; color: #015249;">TOTAL INVERSIÓN EMPRESA</td>
                        <td style="padding: 15px; text-align: right; font-weight: bold; font-size: 18px; color: #015249;">${total_costo:,.0f}</td>
                    </tr>
                </table>

                <h3 style="color: #015249; border-bottom: 2px solid #015249; padding-bottom: 8px; margin-bottom: 5px;">C. Auditoría de Tanqueos en Tránsito</h3>
                <p style="font-size: 13px; color: #666; margin-bottom: 15px;">Operaciones que cuentan con código de pedido asignado, pero aún no registran el ingreso físico (Tanqueo) en la plataforma.</p>
                <table style="width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 10px;">
                    <tr style="background-color: #f8f9fa; text-align: left; border-bottom: 2px solid #ddd;">
                        <th style="padding: 12px;">Fecha Op.</th><th style="padding: 12px;">Ubicación</th><th style="padding: 12px;">Código Pedido</th><th style="padding: 12px;">Última Actividad</th>
                    </tr>
                    {html_audit}
                </table>
            </div>

            <div style="background-color: #1f2937; color: #9ca3af; padding: 20px; text-align: center; font-size: 12px;">
                Este es un informe generado automáticamente.<br>
                <strong>BQA-ONE - Equipo de Gestión Energética</strong>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        msg = MIMEMultipart()
        msg["Subject"] = f"📊 Reporte Semanal GLP - {empresa}"
        msg["From"] = e_from
        
        to_emails = destinatarios if destinatarios else []
        msg["To"] = ", ".join(to_emails) if to_emails else e_admin
            
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))
        
        all_recipients = list(set(to_emails + [e_admin]))

        server = smtplib.SMTP(e_host, e_port)
        server.starttls()
        server.login(e_user, e_pass)
        server.sendmail(e_user, all_recipients, msg.as_string())
        server.quit()
        print(f"📧 Correo enviado exitosamente para {empresa}.")
    except Exception as e:
        print(f"⛔ Error enviando correo para {empresa}: {e}")

if __name__ == "__main__":
    generar_reporte_semanal()