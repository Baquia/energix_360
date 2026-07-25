# app/blueprints/C_bp_mecanico_glp.py
# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify, session
from flask import current_app as app
from app import mysql, csrf
from app.utils import login_required_custom
import json
import os
import base64
import re
from datetime import datetime
import traceback

# Blueprint para operaciones de campo (Grupo C) - PWA
bp_mecanico_glp = Blueprint('bp_mecanico_glp', __name__, url_prefix='/mecanico_glp')

def check_rol():
    """
    Valida de forma estricta que el usuario tenga el perfil autorizado 
    para acceder a la PWA de visitas técnicas.
    """
    perfil = session.get('perfil', '')
    if perfil not in ['mecanico_glp', 'webmaster']:
        return False
    return True

def _verificar_idempotencia(op_id):
    """
    Barrera de idempotencia para la cola IndexedDB.
    Evita la duplicación de visitas si ocurren reintentos por micro-cortes de red.
    """
    if not op_id: return None
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT 1 FROM cg_visitas_mantenimiento WHERE op_id=%s LIMIT 1", (op_id,))
        if cur.fetchone():
            cur.close()
            return jsonify({
                "success": True, 
                "message": "Visita ya registrada previamente (Sincronización recuperada).",
                "op_id": op_id
            }), 200
        cur.close()
    except Exception as e:
        print(f"Error verificando idempotencia en visitas GLP: {e}")
    return None

def _guardar_evidencia(base64_data, carpeta, nombre_archivo):
    """
    Procesa y guarda imágenes capturadas offline (base64) en el sistema de archivos local,
    devolviendo la ruta relativa para la base de datos.
    """
    if not base64_data:
        return None
    try:
        data_match = re.match(r'data:image/(?P<ext>png|jpeg|jpg);base64,(?P<data>.+)', base64_data)
        if not data_match:
            if "," in base64_data:
                base64_data = base64_data.split(",", 1)[1]
            ext = 'jpg'
            binary_data = base64.b64decode(base64_data)
        else:
            ext = data_match.group('ext')
            binary_data = base64.b64decode(data_match.group('data'))

        # Aislamiento por inquilino y equipo
        static_dir = os.path.join(app.static_folder, carpeta) 
        if not os.path.exists(static_dir):
            os.makedirs(static_dir)

        filename = f"{nombre_archivo}.{ext}"
        file_path = os.path.join(static_dir, filename)

        with open(file_path, 'wb') as f:
            f.write(binary_data)

        ruta_relativa = os.path.relpath(file_path, app.static_folder).replace(os.path.sep, "/")
        return f"/static/{ruta_relativa}"
        
    except Exception as e:
        print(f"Error al guardar evidencia fotográfica: {e}")
        return None

# ==========================================
# RUTA PRINCIPAL (PWA)
# ==========================================

@bp_mecanico_glp.route('/visitas', methods=['GET'])
@login_required_custom
def vista_visitas():
    if not check_rol():
        return render_template('403.html', message="No tienes permisos para acceder a las visitas técnicas."), 403
    return render_template('mecanico/visitas_offline.html')

# ==========================================
# ENDPOINTS DE SINCRONIZACIÓN
# ==========================================

@csrf.exempt
@bp_mecanico_glp.route('/sync_down', methods=['POST'])
@login_required_custom
def sync_down():
    """
    Descarga la agenda y los equipos asignados al caché local del dispositivo.
    Garantiza aislamiento multiempresa (SaaS).
    """
    if not check_rol():
        return jsonify({"success": False, "message": "Acceso Denegado."}), 403
        
    empresa_id = session.get('empresa_id')
    
    try:
        cur = mysql.connection.cursor()
        
        # 1. Descargar Inventario de Equipos Activos del Tenant
        cur.execute("""
            SELECT id, tipo, serial_codigo, capacidad, estado 
            FROM cg_equipos 
            WHERE empresa_id = %s AND estado = 'activo'
        """, (empresa_id,))
        equipos_raw = cur.fetchall()
        equipos = [dict(eq) if isinstance(eq, dict) else {
            'id': eq[0], 'tipo': eq[1], 'serial_codigo': eq[2], 'capacidad': eq[3], 'estado': eq[4]
        } for eq in equipos_raw]
        
        # 2. Descargar Agenda de Mantenimiento del Tenant
        cur.execute("""
            SELECT m.id, m.equipo_id, e.serial_codigo, m.fecha_programada, m.tipo_mantenimiento, m.estado
            FROM cg_mantenimientos_programados m
            JOIN cg_equipos e ON m.equipo_id = e.id
            WHERE m.empresa_id = %s AND m.estado IN ('pendiente', 'en_proceso')
        """, (empresa_id,))
        mantenimientos_raw = cur.fetchall()
        mantenimientos = [dict(m) if isinstance(m, dict) else {
            'id': m[0], 'equipo_id': m[1], 'serial_codigo': m[2], 
            'fecha_programada': m[3].strftime('%Y-%m-%d') if hasattr(m[3], 'strftime') else m[3],
            'tipo_mantenimiento': m[4], 'estado': m[5]
        } for m in mantenimientos_raw]
        
        cur.close()
        
        return jsonify({
            "success": True,
            "equipos": equipos,
            "mantenimientos": mantenimientos
        })
        
    except Exception as e:
        print(f"Error en sync_down: {traceback.format_exc()}")
        return jsonify({"success": False, "message": f"Error del servidor: {str(e)}"}), 500


@csrf.exempt
@bp_mecanico_glp.route('/sincronizar_visita', methods=['POST'])
@login_required_custom
def sincronizar_visita():
    """
    Endpoint (Push) que recibe las visitas realizadas sin internet.
    Protegido por barrera de idempotencia op_id.
    """
    if not check_rol():
        return jsonify({"success": False, "message": "Acceso Denegado."}), 403
        
    try:
        data = request.get_json(force=True)
        empresa_id = session.get('empresa_id')
        mecanico_id = session.get('usuario_id')
        
        op_id = data.get('op_id')
        equipo_id = data.get('equipo_id')
        mantenimiento_id = data.get('mantenimiento_programado_id') # Permite None para mantenimientos correctivos exprés
        fecha_visita = data.get('fecha_visita') or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        observaciones = data.get('observaciones', '')
        evidencias_b64 = data.get('evidencias', []) # Array de strings base64
        
        if not op_id or not equipo_id:
            return jsonify({"success": False, "message": "Faltan datos obligatorios (op_id o equipo_id)."}), 400
            
        # 1. Blindaje contra envío duplicado desde PWA
        check = _verificar_idempotencia(op_id)
        if check: return check
        
        cur = mysql.connection.cursor()
        
        # 2. Inserción base transaccional
        cur.execute("""
            INSERT INTO cg_visitas_mantenimiento 
            (empresa_id, mecanico_id, equipo_id, mantenimiento_programado_id, fecha_visita, observaciones, evidencias, op_id, fecha_sincronizacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (empresa_id, mecanico_id, equipo_id, mantenimiento_id, fecha_visita, observaciones, '[]', op_id))
        
        visita_id = cur.lastrowid
        
        # 3. Procesamiento de Fotos (Evidencias JSON)
        rutas_evidencias = []
        if evidencias_b64:
            carpeta = os.path.join("evidencias_cg", str(empresa_id), str(equipo_id))
            for idx, b64 in enumerate(evidencias_b64):
                nombre_archivo = f"visita_{visita_id}_ev_{idx}"
                ruta = _guardar_evidencia(b64, carpeta, nombre_archivo)
                if ruta:
                    rutas_evidencias.append(ruta)
        
        # 4. Actualización del registro con las rutas finales
        if rutas_evidencias:
            cur.execute("""
                UPDATE cg_visitas_mantenimiento SET evidencias = %s WHERE id = %s
            """, (json.dumps(rutas_evidencias), visita_id))
            
        # 5. Cierre del mantenimiento en la agenda maestro
        if mantenimiento_id:
            cur.execute("""
                UPDATE cg_mantenimientos_programados 
                SET estado = 'finalizado' 
                WHERE id = %s AND empresa_id = %s
            """, (mantenimiento_id, empresa_id))
            
        mysql.connection.commit()
        cur.close()
        
        return jsonify({
            "success": True, 
            "message": "Visita técnica sincronizada exitosamente.",
            "op_id": op_id
        })
        
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error sincronizando visita desde PWA: {traceback.format_exc()}")
        return jsonify({"success": False, "message": f"Error del servidor: {str(e)}"}), 500