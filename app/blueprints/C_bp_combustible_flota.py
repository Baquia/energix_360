# app/blueprints/C_bp_combustible_flota.py
import os
from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from app import mysql
from app.utils import login_required_custom
from datetime import datetime
import MySQLdb.cursors

bp_combustible_flota = Blueprint('combustible_flota', __name__)

@bp_combustible_flota.route('/combustible', methods=['GET', 'POST'])
@login_required_custom
def registrar_combustible():
    empresa_id = session.get('empresa_id')
    
    if request.method == 'POST':
        placa = str(request.form.get('placa', '')).upper().strip()
        tipo_combustible = request.form.get('tipo_combustible', '').strip()
        fecha_tanqueo = request.form.get('fecha_tanqueo')
        kilometraje = request.form.get('kilometraje')
        galones = request.form.get('galones')
        valor_total = request.form.get('valor_total')
        
        usuario_id = session.get('usuario_id')
        nombre_operador = session.get('nombre')
        empresa_nombre = session.get('empresa')

        if not all([placa, tipo_combustible, fecha_tanqueo, kilometraje, galones, valor_total]):
            flash("Por favor, complete todos los campos obligatorios.", "danger")
            return redirect(url_for('combustible_flota.registrar_combustible'))

        # CANDADO MULTIEMPRESA: Verificar que la placa pertenece a la empresa de la sesión
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT id FROM vehiculos WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
        vehiculo = cur.fetchone()
        
        if not vehiculo:
            cur.close()
            flash("El vehículo seleccionado no es válido o no pertenece a su empresa.", "danger")
            return redirect(url_for('combustible_flota.registrar_combustible'))

        try:
            cur.execute("""
                INSERT INTO vehiculos_combustible_flota (
                    id_empresa, empresa, placa, tipo_combustible, fecha_tanqueo, 
                    kilometraje_actual, galones, valor_total, id_operador, nombre_operador
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                empresa_id, empresa_nombre, placa, tipo_combustible, fecha_tanqueo,
                kilometraje, galones, valor_total, usuario_id, nombre_operador
            ))
            mysql.connection.commit()
            flash("Registro de tanqueo guardado exitosamente.", "success")
            
            # Retorna al enrutador maestro
            return redirect(url_for('router_universal', modulo='flota'))
            
        except Exception as e:
            mysql.connection.rollback()
            flash(f"Error al guardar el registro: {str(e)}", "danger")
        finally:
            cur.close()

        return redirect(url_for('combustible_flota.registrar_combustible'))

    # MÉTODO GET: Cargar vehículos de la empresa para el selector
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT placa, referencia FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'C_combustible_flota.html',
        vehiculos=vehiculos_db,
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        nit=session.get('empresa_id'),
        hoy=datetime.now().strftime('%Y-%m-%d')
    )