# MÓDULO: MANTENIMIENTO | SUBMÓDULO: CONTROLADOR MANTENIMIENTO | CONDICIÓN: OBLIGATORIO | TIPO: 1

from flask import Blueprint, render_template, session, jsonify, send_file, current_app
from app import mysql
import MySQLdb.cursors
import json
import io
import tempfile
import os
import base64
from app.utils import login_required_custom

# Intentamos importar fpdf para la generación del PDF. Si el ecosistema usa otra (weasyprint/pdfkit), 
# se puede adaptar fácilmente. FPDF es la más ligera y estándar para reportes puros.
try:
    from fpdf import FPDF
except ImportError:
    FPDF = None

bp_controlador_mantenimiento = Blueprint('controlador_mantenimiento', __name__)

# ==============================================================================
# VISTA PRINCIPAL (HUB CONTROLADOR)
# ==============================================================================
@bp_controlador_mantenimiento.route('/B_controlador_mantenimiento.html')
@bp_controlador_mantenimiento.route('/hub_inspecciones_tecnicas_clientes.html')
@login_required_custom
def vista_controlador():
    """
    Renderiza la interfaz principal del Controlador de Mantenimiento.
    Validación de perfil implicita en el ruteo del Login (Arquitectura P&P).
    """
    usuario = session.get('nombre', 'Controlador')
    empresa = session.get('empresa', 'Empresa')
    nit = str(session.get('empresa_id', ''))

    return render_template('B_controlador_mantenimiento.html',
                           usuario=usuario,
                           empresa=empresa,
                           nit=nit)

# ==============================================================================
# API: LISTAR INSPECCIONES (MULTI-TENANT)
# ==============================================================================
@bp_controlador_mantenimiento.route('/api/mantenimiento/listar_inspecciones', methods=['GET'])
@login_required_custom
def listar_inspecciones():
    """
    Retorna la lista consolidada de inspecciones registradas para la empresa actual.
    """
    empresa_id = str(session.get('empresa_id'))
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # JOIN con la tabla usuarios (si existe) para traer el nombre real del operador,
        # de lo contrario, se usa el usuario_id.
        cur.execute("""
            SELECT 
                i.id, 
                i.id_local, 
                i.cliente, 
                i.ubicacion, 
                DATE_FORMAT(i.fecha_inspeccion, '%%Y-%%m-%%d %%H:%%i') as fecha_inspeccion,
                i.usuario_id,
                u.nombre as nombre_operador
            FROM mantenimiento_inspecciones i
            LEFT JOIN usuarios u ON i.usuario_id = u.id AND i.id_empresa = u.empresa_id
            WHERE i.id_empresa = %s
            ORDER BY i.fecha_inspeccion DESC, i.id DESC
            LIMIT 100
        """, (empresa_id,))
        
        inspecciones = cur.fetchall()
        cur.close()
        
        return jsonify(inspecciones)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==============================================================================
# API: DETALLE DE INSPECCIÓN
# ==============================================================================
@bp_controlador_mantenimiento.route('/api/mantenimiento/detalle_inspeccion/<int:id_inspeccion>', methods=['GET'])
@login_required_custom
def detalle_inspeccion(id_inspeccion):
    """
    Retorna la cabecera completa (incluyendo firmas) y los ítems de una inspección específica.
    Strict Multi-Tenant Check.
    """
    empresa_id = str(session.get('empresa_id'))
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # 1. Traer Cabecera
        cur.execute("""
            SELECT 
                i.*, 
                DATE_FORMAT(i.fecha_inspeccion, '%%Y-%%m-%%d %%H:%%i') as fecha_inspeccion,
                u.nombre as nombre_operador
            FROM mantenimiento_inspecciones i
            LEFT JOIN usuarios u ON i.usuario_id = u.id AND i.id_empresa = u.empresa_id
            WHERE i.id = %s AND i.id_empresa = %s
        """, (id_inspeccion, empresa_id))
        
        cabecera = cur.fetchone()
        
        if not cabecera:
            cur.close()
            return jsonify({'error': 'Inspección no encontrada o sin permisos.'}), 404
            
        # 2. Traer Ítems
        cur.execute("""
            SELECT id, nombre, hallazgo, foto_base64 
            FROM mantenimiento_inspeccion_items
            WHERE id_inspeccion = %s AND id_empresa = %s
            ORDER BY id ASC
        """, (id_inspeccion, empresa_id))
        
        items = cur.fetchall()
        cur.close()
        
        return jsonify({
            'status': 'ok',
            'cabecera': cabecera,
            'items': items
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==============================================================================
# API: DESCARGAR REPORTE EN PDF
# ==============================================================================
# ==============================================================================
# API: DESCARGAR REPORTE EN PDF
# ==============================================================================
# ==============================================================================
# CLASE PDF PERSONALIZADA (DIAGRAMACIÓN BQA-ONE)
# ==============================================================================
if FPDF is not None:
    class BQAReportPDF(FPDF):
        def __init__(self, id_inspeccion, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.id_inspeccion = id_inspeccion

        def header(self):
            # Inyección de Logo BQA-ONE (Ajustado al borde izquierdo y escalado -20%)
            logo_path = os.path.join(current_app.static_folder, 'logo_energix360.png')
            if os.path.exists(logo_path):
                self.image(logo_path, x=10, y=8, w=28)
            
            # Título y metadatos derechos
            self.set_font("Arial", 'B', 14)
            self.set_text_color(0, 51, 102) # Primary Dark BQA
            self.cell(0, 6, "REPORTE DE INSPECCIÓN TÉCNICA", border=0, ln=True, align='R')
            self.set_font("Arial", '', 9)
            self.set_text_color(100, 100, 100)
            self.cell(0, 5, f"ID Documento: {self.id_inspeccion}", border=0, ln=True, align='R')
            self.cell(0, 5, "Módulo de Mantenimiento", border=0, ln=True, align='R')
            self.ln(10)
            self.set_draw_color(226, 232, 240) # Border color
            self.line(15, self.get_y(), 195, self.get_y())
            self.ln(5)

        def footer(self):
            self.set_y(-20)
            self.set_draw_color(226, 232, 240)
            self.line(15, self.get_y(), 195, self.get_y())
            self.ln(2)
            self.set_font("Arial", 'B', 7)
            self.set_text_color(0, 78, 146)
            self.cell(0, 4, "Generado con tecnología BQA-ONE - Sistema Master de Gestión Empresarial", align='C', ln=True)
            self.set_font("Arial", 'I', 7)
            self.set_text_color(128, 128, 128)
            self.cell(0, 4, f"Página {self.page_no()} de {{nb}}", align='C')

# ==============================================================================
# API: DESCARGAR REPORTE EN PDF
# ==============================================================================
@bp_controlador_mantenimiento.route('/api/mantenimiento/pdf_inspeccion/<int:id_inspeccion>', methods=['GET'])
@login_required_custom
def pdf_inspeccion(id_inspeccion):
    """
    Genera y retorna un archivo PDF con el reporte completo de la inspección,
    leyendo las fotografías y firmas desde las rutas físicas del servidor,
    con diagramación avanzada BQA-ONE, georreferenciación y sello de autenticidad.
    """
    if FPDF is None:
        return jsonify({'error': 'Librería FPDF no instalada en el backend.'}), 500

    empresa_id = str(session.get('empresa_id'))
    empresa_nombre = session.get('empresa', 'Empresa')
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # Consultar cabecera (incluye coordenadas y operador)
        cur.execute("""
            SELECT 
                i.*, 
                DATE_FORMAT(i.fecha_inspeccion, '%%Y-%%m-%%d %%H:%%i') as fecha_fmt,
                u.nombre as nombre_operador
            FROM mantenimiento_inspecciones i
            LEFT JOIN usuarios u ON i.usuario_id = u.id AND i.id_empresa = u.empresa_id
            WHERE i.id = %s AND i.id_empresa = %s
        """, (id_inspeccion, empresa_id))
        cabecera = cur.fetchone()
        
        if not cabecera:
            cur.close()
            return jsonify({'error': 'Registro no encontrado.'}), 404
            
        # Consultar ítems (Incluyendo observaciones)
        cur.execute("""
            SELECT nombre, hallazgo, observaciones, foto_base64 
            FROM mantenimiento_inspeccion_items
            WHERE id_inspeccion = %s AND id_empresa = %s
            ORDER BY id ASC
        """, (id_inspeccion, empresa_id))
        items = cur.fetchall()
        cur.close()
        
        # --- INICIO GENERACIÓN PDF ---
        pdf = BQAReportPDF(id_inspeccion=id_inspeccion)
        pdf.alias_nb_pages()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=25)
        
        # Helper: Convertir ruta relativa de BD (/static/...) a ruta física absoluta
        def get_abs_path(db_path):
            if not db_path:
                return None
            if db_path.startswith('/static/'):
                rel_path = db_path[8:] # Remueve '/static/'
                abs_path = os.path.join(current_app.static_folder, rel_path)
                if os.path.exists(abs_path):
                    return abs_path
            return None

        # --- TABLA DE DATOS GENERALES ---
        pdf.set_fill_color(248, 250, 252)
        pdf.set_draw_color(226, 232, 240)
        
        datos = [
            ("Empresa Operadora:", empresa_nombre),
            ("Operador / Técnico:", cabecera['nombre_operador'] or cabecera['usuario_id']),
            ("Cliente Inspeccionado:", cabecera['cliente']),
            ("Ubicación / Área:", cabecera.get('ubicacion') or 'N/A'),
            ("Fecha Inspección:", cabecera['fecha_fmt'])
        ]
        
        # Georreferenciación
        lat = cabecera.get('latitud')
        lng = cabecera.get('longitud')
        coord_text = f"{lat}, {lng}" if lat and lng else "No registrada"
        datos.append(("Georreferenciación (GPS):", coord_text))

        for titulo, valor in datos:
            pdf.set_font("Arial", 'B', 9)
            pdf.set_text_color(71, 85, 105)
            pdf.cell(55, 8, f" {titulo}", border=1, fill=True)
            pdf.set_font("Arial", '', 9)
            pdf.set_text_color(15, 23, 42)
            pdf.cell(125, 8, f" {valor}", border=1, ln=True)
            
        pdf.ln(8)
        
        # --- DETALLE DE HALLAZGOS ---
        pdf.set_font("Arial", 'B', 11)
        pdf.set_text_color(0, 51, 102)
        pdf.cell(0, 8, "DETALLE DE HALLAZGOS E INSPECCIÓN VISUAL", ln=True)
        pdf.line(15, pdf.get_y(), 195, pdf.get_y())
        pdf.ln(5)
        
        for idx, item in enumerate(items, 1):
            pdf.set_fill_color(241, 245, 249)
            pdf.set_font("Arial", 'B', 9)
            pdf.set_text_color(15, 23, 42)
            
            estado = str(item['hallazgo']).replace('_', ' ').upper()
            
            # Fila de Título del ítem
            pdf.cell(180, 7, f" Ítem {idx}: {item['nombre']}  |  Estado: {estado}", border=1, ln=True, fill=True)
            
            if item.get('observaciones'):
                pdf.set_font("Arial", '', 9)
                pdf.set_text_color(71, 85, 105)
                pdf.multi_cell(180, 6, f" Observaciones: {item['observaciones']}", border=1)
            
            img_path = get_abs_path(item['foto_base64'])
            if img_path:
                y_img = pdf.get_y() + 2
                try:
                    pdf.image(img_path, x=17, y=y_img, w=50)
                    pdf.cell(180, 55, "", border=1, ln=True) # Espacio para la foto
                except Exception:
                    pdf.set_font("Arial", 'I', 8)
                    pdf.cell(180, 10, " (Error al procesar la evidencia fotográfica)", border=1, ln=True)
            else:
                pdf.set_font("Arial", 'I', 8)
                pdf.cell(180, 10, " (Sin evidencia fotográfica válida adjunta)", border=1, ln=True)
                
            pdf.ln(4)
            
            if pdf.get_y() > 220:
                pdf.add_page()
                
        pdf.ln(5)
        
        # --- FIRMAS ---
        if pdf.get_y() > 180:
            pdf.add_page()
            
        pdf.set_font("Arial", 'B', 11)
        pdf.set_text_color(0, 51, 102)
        pdf.cell(0, 8, "CIERRE Y CONFORMIDAD", ln=True)
        pdf.line(15, pdf.get_y(), 195, pdf.get_y())
        pdf.ln(5)
        
        pdf.set_font("Arial", 'B', 9)
        pdf.set_text_color(15, 23, 42)
        pdf.cell(0, 8, f"Encargado del Área (Cliente): {cabecera['encargado_nombre']}", ln=True)
        
        y_pos = pdf.get_y()
        pdf.set_font("Arial", 'B', 8)
        pdf.set_text_color(71, 85, 105)
        
        # 3 Columnas para Firmas y Foto
        pdf.set_xy(15, y_pos)
        pdf.cell(50, 5, "Firma del Encargado", align='C')
        pdf.set_xy(75, y_pos)
        pdf.cell(50, 5, "Firma del Operador", align='C')
        pdf.set_xy(135, y_pos)
        pdf.cell(50, 5, "Fotografía / Acta Física", align='C')
        pdf.ln(5)
        
        firma_cli = get_abs_path(cabecera.get('firma_base64'))
        firma_op = get_abs_path(cabecera.get('firma_operador_base64'))
        foto_enc = get_abs_path(cabecera.get('foto_encargado_base64'))
        
        img_y = y_pos + 10
        if firma_cli:
            try: pdf.image(firma_cli, x=15, y=img_y, w=50)
            except: pass
                
        if firma_op:
            try: pdf.image(firma_op, x=75, y=img_y, w=50)
            except: pass

        if foto_enc:
            try: pdf.image(foto_enc, x=135, y=img_y, w=50)
            except: pass
            
        pdf.set_y(y_pos + 65)

        # --- SELLO DE AUTENTICIDAD ---
        pdf.ln(5)
        pdf.set_fill_color(240, 253, 244) # Fondo verde muy claro
        pdf.set_draw_color(34, 197, 94)   # Borde verde
        pdf.rect(15, pdf.get_y(), 180, 22, style='DF')
        
        from datetime import datetime
        fecha_emision = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        pdf.set_y(pdf.get_y() + 4)
        pdf.set_font("Arial", 'B', 9)
        pdf.set_text_color(21, 128, 61) # Verde oscuro
        pdf.cell(180, 5, "DOCUMENTO VERIFICADO Y REGISTRADO", align='C', ln=True)
        pdf.set_font("Arial", '', 7)
        pdf.cell(180, 4, f"Emitido: {fecha_emision} | Tracking ID: {cabecera['id_local']}", align='C', ln=True)
        pdf.cell(180, 4, "Los datos contenidos en este documento son inmutables tras su sincronización.", align='C', ln=True)
                
        pdf_bytes = pdf.output(dest='S').encode('latin1')
                
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f"Reporte_Inspeccion_{id_inspeccion}.pdf"
        )
        
    except Exception as e:
        return jsonify({'error': f'Error al generar PDF: {str(e)}'}), 500