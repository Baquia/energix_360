# app/blueprints/B_bp_comercializador_glp.py
# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify, session
from app import mysql, csrf
from app.utils import login_required_custom
import json
from datetime import datetime
import traceback

# Blueprint para operaciones administrativas del Grupo B
bp_comercializador_glp = Blueprint('bp_comercializador_glp', __name__, url_prefix='/comercializador')

def check_rol():
    """
    Valida de forma estricta que el usuario tenga el perfil autorizado 
    para acceder a las funciones administrativas del Comercializador.
    """
    perfil = session.get('perfil', '')
    if perfil not in ['controlador_comercializadorglp', 'webmaster']:
        return False
    return True

# ==========================================
# SUBMÓDULO 1: MAESTRO DE EQUIPOS
# ==========================================

@bp_comercializador_glp.route('/equipos', methods=['GET'])
@login_required_custom
def vista_equipos():
    if not check_rol():
        return render_template('403.html', message="No tienes permisos para acceder al maestro de equipos."), 403
        
    empresa_id = session.get('empresa_id')
    
    try:
        cur = mysql.connection.cursor()
        # Consulta multi-tenant inamovible
        cur.execute("""
            SELECT id, tipo, serial_codigo, capacidad, estado, especificaciones_tecnicas, fecha_registro 
            FROM cg_equipos 
            WHERE empresa_id = %s 
            ORDER BY fecha_registro DESC
        """, (empresa_id,))
        equipos = cur.fetchall()
        cur.close()
        
        # Transformar JSON string a diccionarios de Python para la vista
        equipos_procesados = []
        for eq in equipos:
            eq_dict = dict(eq) if isinstance(eq, dict) else {
                'id': eq[0], 'tipo': eq[1], 'serial_codigo': eq[2], 
                'capacidad': eq[3], 'estado': eq[4], 
                'especificaciones_tecnicas': eq[5], 'fecha_registro': eq[6]
            }
            
            try:
                if eq_dict['especificaciones_tecnicas']:
                    eq_dict['especificaciones_dict'] = json.loads(eq_dict['especificaciones_tecnicas'])
                else:
                    eq_dict['especificaciones_dict'] = {}
            except Exception:
                eq_dict['especificaciones_dict'] = {}
                
            equipos_procesados.append(eq_dict)
            
        return render_template('comercializador/equipos.html', equipos=equipos_procesados)
    except Exception as e:
        print(f"Error cargando vista de equipos: {e}")
        return "Error interno del servidor", 500


@csrf.exempt
@bp_comercializador_glp.route('/equipos/guardar', methods=['POST'])
@login_required_custom
def guardar_equipo():
    if not check_rol():
        return jsonify({"success": False, "message": "Acceso Denegado. Rol no autorizado."}), 403
        
    try:
        data = request.get_json(force=True)
        empresa_id = session.get('empresa_id')
        
        tipo = data.get('tipo')
        serial_codigo = data.get('serial_codigo')
        capacidad = float(data.get('capacidad', 0))
        estado = data.get('estado', 'activo')
        
        # Flexible para escalar a múltiples atributos dinámicos
        especificaciones = json.dumps(data.get('especificaciones_tecnicas', {}))
        
        if not tipo or not serial_codigo:
            return jsonify({"success": False, "message": "Los campos Tipo y Serial/Código son obligatorios."}), 400
            
        cur = mysql.connection.cursor()
        
        equipo_id = data.get('id')
        if equipo_id:
            # Actualización (Asegurando blindaje de tenant)
            cur.execute("""
                UPDATE cg_equipos 
                SET tipo=%s, serial_codigo=%s, capacidad=%s, estado=%s, especificaciones_tecnicas=%s
                WHERE id=%s AND empresa_id=%s
            """, (tipo, serial_codigo, capacidad, estado, especificaciones, equipo_id, empresa_id))
            msg = "Equipo actualizado correctamente."
        else:
            # Creación
            fecha_registro = datetime.now()
            cur.execute("""
                INSERT INTO cg_equipos (empresa_id, tipo, serial_codigo, capacidad, estado, especificaciones_tecnicas, fecha_registro)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (empresa_id, tipo, serial_codigo, capacidad, estado, especificaciones, fecha_registro))
            msg = "Equipo registrado correctamente."
            
        mysql.connection.commit()
        cur.close()
        
        return jsonify({"success": True, "message": msg})
        
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error guardando equipo: {traceback.format_exc()}")
        return jsonify({"success": False, "message": f"Error del servidor: {str(e)}"}), 500


# ==========================================
# SUBMÓDULO 2: PROGRAMACIÓN DE MANTENIMIENTO
# ==========================================

@bp_comercializador_glp.route('/mantenimientos', methods=['GET'])
@login_required_custom
def vista_mantenimientos():
    if not check_rol():
        return render_template('403.html', message="No tienes permisos para programar mantenimientos."), 403
        
    empresa_id = session.get('empresa_id')
    
    try:
        cur = mysql.connection.cursor()
        
        # Obtener agenda de mantenimientos vinculada a los equipos
        cur.execute("""
            SELECT m.id, m.equipo_id, e.serial_codigo, e.tipo, m.fecha_programada, m.estado, m.tipo_mantenimiento 
            FROM cg_mantenimientos_programados m
            JOIN cg_equipos e ON m.equipo_id = e.id
            WHERE m.empresa_id = %s
            ORDER BY m.fecha_programada ASC
        """, (empresa_id,))
        mantenimientos = cur.fetchall()
        
        # Obtener lista de equipos activos para llenar el Select del Modal
        cur.execute("""
            SELECT id, serial_codigo, tipo 
            FROM cg_equipos 
            WHERE empresa_id = %s AND estado = 'activo'
        """, (empresa_id,))
        equipos = cur.fetchall()
        
        cur.close()
        
        return render_template('comercializador/mantenimientos.html', mantenimientos=mantenimientos, equipos=equipos)
    except Exception as e:
        print(f"Error cargando vista de mantenimientos: {e}")
        return "Error interno del servidor", 500


@csrf.exempt
@bp_comercializador_glp.route('/mantenimientos/programar', methods=['POST'])
@login_required_custom
def programar_mantenimiento():
    if not check_rol():
        return jsonify({"success": False, "message": "Acceso Denegado. Rol no autorizado."}), 403
        
    try:
        data = request.get_json(force=True)
        empresa_id = session.get('empresa_id')
        
        equipo_id = data.get('equipo_id')
        fecha_programada = data.get('fecha_programada')
        tipo_mantenimiento = data.get('tipo_mantenimiento')
        estado = 'pendiente'
        
        if not equipo_id or not fecha_programada or not tipo_mantenimiento:
            return jsonify({"success": False, "message": "Todos los campos son obligatorios para programar un mantenimiento."}), 400
            
        cur = mysql.connection.cursor()
        
        # Validar de forma estricta que el equipo pertenezca al inquilino (Tenant)
        cur.execute("SELECT id FROM cg_equipos WHERE id = %s AND empresa_id = %s", (equipo_id, empresa_id))
        if not cur.fetchone():
            cur.close()
            return jsonify({"success": False, "message": "El equipo seleccionado no existe o no pertenece a tu empresa."}), 403
        
        cur.execute("""
            INSERT INTO cg_mantenimientos_programados (empresa_id, equipo_id, fecha_programada, estado, tipo_mantenimiento)
            VALUES (%s, %s, %s, %s, %s)
        """, (empresa_id, equipo_id, fecha_programada, estado, tipo_mantenimiento))
        
        mysql.connection.commit()
        cur.close()
        
        return jsonify({"success": True, "message": "Mantenimiento programado correctamente en el calendario."})
        
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error programando mantenimiento: {traceback.format_exc()}")
        return jsonify({"success": False, "message": f"Error del servidor: {str(e)}"}), 500