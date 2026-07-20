from flask import Blueprint, render_template, request, jsonify, session, redirect, send_file
import io
from app import mysql, csrf
import pandas as pd
import re
from datetime import datetime, timedelta
import MySQLdb.cursors
import unicodedata
import math

bp_bodegas = Blueprint('bodegas', __name__)

# ==============================================================================
# 1. VISTA PRINCIPAL (DASHBOARD CONTROLADOR/JEFE)
# ==============================================================================

@bp_bodegas.route('/B_modulo_controlador_bodegas.html')
def control_logistica():
    if 'usuario_id' not in session: return redirect('/')
    
    empresa_id = session.get('empresa_id')

    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fabricantes_proveedores (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_empresa INT NOT NULL,
                marca VARCHAR(100) NOT NULL,
                operador_asignado INT NOT NULL,
                UNIQUE KEY unique_marca_empresa (id_empresa, marca)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        mysql.connection.commit()
        cur.close()
    except Exception as e:
        print(f"Aviso tabla fabricantes_proveedores: {e}")
    
    kpis = {
        'pedidos_totales': 0, 
        'total_items_pendientes': 0, 
        'items_finalizados': 0, 
        'pedidos_pendientes_reales': 0,
        'velocidad_promedio': 0.0,
        'requiere_alias': 0,
        'backlog_verificacion': 0
    }
    
    ordenes_sin_asignar = [] 
    ordenes_procesadas = []
    operarios_marcas = [] 
    verificadores_kpis = []
    marcas_huerfanas = []
    alertas_criticas = []

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # KPIs Generales
        cur.execute("""
            SELECT 
                COUNT(DISTINCT numero_orden_origen) as total,
                SUM(CASE WHEN estado_actividad NOT IN ('VERIFICADO', 'DESPACHADO', 'TERMINADO') OR estado_actividad IS NULL THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado_actividad IN ('VERIFICADO', 'DESPACHADO') THEN 1 ELSE 0 END) as listos
            FROM picking_importacion_raw 
            WHERE id_empresa = %s AND (estado_actividad != 'TERMINADO' OR estado_actividad IS NULL)
        """, (empresa_id,))
        row = cur.fetchone()
        if row:
            kpis['pedidos_totales'] = row['total'] or 0
            kpis['items_pendientes'] = int(row['pendientes'] or 0)
            kpis['items_finalizados'] = int(row['listos'] or 0)

        # KPIs Alias (Excepciones IA)
        cur.execute("""
            SELECT COUNT(*) as requiere_alias 
            FROM picking_importacion_raw 
            WHERE id_empresa = %s AND autorizacion_alistamiento = FALSE AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL)
        """, (empresa_id,))
        row_alias = cur.fetchone()
        if row_alias:
            kpis['requiere_alias'] = int(row_alias['requiere_alias'])

        # Alertas Críticas 
        cur.execute("""
            SELECT 
                numero_orden_origen as orden, codigo_producto, descripcion_producto,
                novedad_alistamiento, nombre_auxiliar_asignado as operario, fecha_fin_alistamiento
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND novedad_alistamiento IS NOT NULL AND (estado_actividad NOT IN ('DESPACHADO', 'TERMINADO') OR estado_actividad IS NULL)
            ORDER BY fecha_fin_alistamiento DESC LIMIT 10
        """, (empresa_id,))
        alertas_criticas = cur.fetchall()

        # Órdenes SIN ASIGNAR PUERTA
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                MAX(puerta_asignada) as puerta_asignada,
                MAX(zona) as zona,
                MAX(secuencia_alistamiento) as secuencia,
                COUNT(*) as total_items,
                MAX(fecha_carga) as fecha
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND puerta_asignada IS NULL AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL) AND autorizacion_alistamiento = TRUE
            GROUP BY numero_orden_origen
            ORDER BY MAX(secuencia_alistamiento) ASC, MAX(fecha_carga) DESC
        """, (empresa_id,))
        ordenes_sin_asignar = cur.fetchall()

        # Operarios y KPIs
        cur.execute("""
            SELECT 
                u.id as id_operario,
                u.nombre as nombre_operario,
                GROUP_CONCAT(DISTINCT fp.marca SEPARATOR ', ') as marcas_asignadas,
                COUNT(DISTINCT fp.marca) as total_marcas,
                COUNT(p.id) as total_asignados,
                SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_completados,
                SUM(CASE WHEN p.novedad_alistamiento IS NOT NULL THEN 1 ELSE 0 END) as novedades,
                SUM(CASE WHEN p.fecha_inicio_alistamiento IS NOT NULL AND p.fecha_fin_alistamiento IS NOT NULL 
                         THEN TIMESTAMPDIFF(MINUTE, p.fecha_inicio_alistamiento, p.fecha_fin_alistamiento) ELSE 0 END) as minutos_totales
            FROM usuarios u
            LEFT JOIN fabricantes_proveedores fp ON u.id = fp.operador_asignado AND fp.id_empresa = %s
            LEFT JOIN picking_importacion_raw p ON p.marca = fp.marca AND p.id_empresa = %s AND p.puerta_asignada IS NOT NULL AND (p.estado_actividad != 'TERMINADO' OR p.estado_actividad IS NULL) AND p.autorizacion_alistamiento = TRUE
            WHERE u.empresa_id = %s AND u.perfil = 'operador_logistica'
            GROUP BY u.id, u.nombre
            ORDER BY u.nombre ASC
        """, (empresa_id, empresa_id, empresa_id))
        operarios_marcas_raw = cur.fetchall()

        for op in operarios_marcas_raw:
            op_dict = dict(op)
            asignados = int(op_dict['total_asignados'] or 0)
            completados = int(op_dict['items_completados'] or 0)
            novedades = int(op_dict['novedades'] or 0)
            mins = float(op_dict['minutos_totales'] or 0)
            
            op_dict['avance_porcentaje'] = round((completados / asignados) * 100) if asignados > 0 else 0
            op_dict['velocidad'] = round(completados / mins, 2) if mins > 0 else 0.0
            op_dict['indice_novedades'] = round((novedades / completados) * 100, 1) if completados > 0 else 0.0
            
            operarios_marcas.append(op_dict)

        # KPIs Verificadores
        cur.execute("""
            SELECT 
                u.id as id_verificador,
                u.nombre as nombre_verificador,
                COUNT(p.id) as items_verificados,
                SUM(CASE WHEN p.cajas_alistadas != p.cajas_verificadas OR p.unidades_alistadas != p.unidades_verificadas THEN 1 ELSE 0 END) as discrepancias
            FROM usuarios u
            LEFT JOIN picking_importacion_raw p ON p.id_verificador = u.id AND p.id_empresa = %s AND DATE(p.fecha_verificacion) = CURDATE()
            WHERE u.empresa_id = %s AND u.perfil IN ('verificador', 'supervisor', 'controlador_logistica')
            GROUP BY u.id, u.nombre
            HAVING items_verificados > 0
            ORDER BY items_verificados DESC
        """, (empresa_id, empresa_id))
        verificadores_raw = cur.fetchall()

        for v in verificadores_raw:
            v_dict = dict(v)
            verificados = int(v_dict['items_verificados'] or 0)
            discrepancias = int(v_dict['discrepancias'] or 0)
            v_dict['indice_discrepancia'] = round((discrepancias / verificados) * 100, 1) if verificados > 0 else 0.0
            verificadores_kpis.append(v_dict)

        # Backlog de Verificación
        cur.execute("""
            SELECT COUNT(id) as total_backlog 
            FROM picking_importacion_raw 
            WHERE id_empresa = %s AND estado_actividad = 'ALISTADO' AND autorizacion_alistamiento = TRUE
        """, (empresa_id,))
        row_backlog = cur.fetchone()
        if row_backlog:
            kpis['backlog_verificacion'] = int(row_backlog['total_backlog'])

        # Marcas Huérfanas
        cur.execute("""
            SELECT p.marca, COUNT(*) as total_items
            FROM picking_importacion_raw p
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa = %s AND fp.operador_asignado IS NULL AND (p.estado_actividad = 'PENDIENTE' OR p.estado_actividad IS NULL) AND p.autorizacion_alistamiento = TRUE
            GROUP BY p.marca
            ORDER BY total_items DESC
        """, (empresa_id,))
        marcas_huerfanas = cur.fetchall()

        # Órdenes ACTIVAS 
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                MAX(puerta_asignada) as puerta_asignada,
                MAX(id_vehiculo) as id_vehiculo,
                MAX(zona) as zona,
                MAX(secuencia_alistamiento) as secuencia,
                COUNT(*) as total_items,
                SUM(CASE WHEN estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_listos,
                SUM(CASE WHEN estado_actividad IN ('VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_verificados,
                SUM(CASE WHEN estado_actividad IN ('DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_despachados,
                MIN(fecha_inicio_alistamiento) as inicio,
                MAX(fecha_fin_alistamiento) as fin,
                SUM(CASE WHEN fecha_inicio_alistamiento IS NOT NULL AND fecha_fin_alistamiento IS NOT NULL THEN TIMESTAMPDIFF(MINUTE, fecha_inicio_alistamiento, fecha_fin_alistamiento) ELSE 0 END) as minutos_totales
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND puerta_asignada IS NOT NULL AND autorizacion_alistamiento = TRUE
            GROUP BY numero_orden_origen
            HAVING SUM(CASE WHEN estado_actividad IN ('ASIGNADO', 'EN_PROCESO', 'ALISTADO', 'VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) > 0
        """, (empresa_id,))
        raw_ordenes = cur.fetchall()
        
        activos = []
        finalizados = []
        ahora = datetime.now()
        
        total_items_speed = 0
        total_mins_speed = 0

        for o in raw_ordenes:
            total_it = int(o['total_items'] or 0)
            it_listos = int(o['items_listos'] or 0)
            it_verif = int(o['items_verificados'] or 0)
            it_desp = int(o['items_despachados'] or 0)

            es_alistado = (it_listos == total_it) and (total_it > 0)
            es_verificado = (it_verif == total_it) and (total_it > 0)
            es_despachado = (it_desp == total_it) and (total_it > 0)
            
            o['velocidad'] = 0.0
            mins = float(o['minutos_totales'] or 0)
            if mins > 0:
                o['velocidad'] = round(it_listos / mins, 2)
                total_items_speed += it_listos
                total_mins_speed += mins
            elif it_listos > 0 and o['inicio']:
                try:
                    inicio_dt = o['inicio'] if isinstance(o['inicio'], datetime) else datetime.strptime(str(o['inicio']), '%Y-%m-%d %H:%M:%S')
                    elapsed = (ahora - inicio_dt).total_seconds() / 60.0
                    if elapsed > 0:
                        o['velocidad'] = round(it_listos / elapsed, 2)
                        total_items_speed += it_listos
                        total_mins_speed += elapsed
                except Exception:
                    pass

            o['duracion_str'] = "--:--"
            if es_alistado and o['inicio'] and o['fin']:
                try:
                    inicio_dt = o['inicio'] if isinstance(o['inicio'], datetime) else datetime.strptime(str(o['inicio']), '%Y-%m-%d %H:%M:%S')
                    fin_dt = o['fin'] if isinstance(o['fin'], datetime) else datetime.strptime(str(o['fin']), '%Y-%m-%d %H:%M:%S')
                    diff = fin_dt - inicio_dt
                    horas = int(diff.total_seconds()) // 3600
                    minutos = (int(diff.total_seconds()) % 3600) // 60
                    segundos = int(diff.total_seconds()) % 60
                    o['duracion_str'] = f"{horas:02}:{minutos:02}:{segundos:02}"
                except Exception:
                    pass

            if es_despachado:
                o['estado_visual'] = 'DESPACHADO'
                o['color_fila'] = '#f0fdf4'
                try:
                    fecha_cierre = o['fin'] if isinstance(o['fin'], datetime) else (datetime.strptime(str(o['fin']), '%Y-%m-%d %H:%M:%S') if o['fin'] else ahora)
                    if (ahora - fecha_cierre) < timedelta(hours=24):
                        finalizados.append(o)
                except:
                    finalizados.append(o)
            elif es_verificado:
                o['estado_visual'] = 'VERIFICADO'
                o['color_fila'] = '#f0fdf4'
                try:
                    fecha_cierre = o['fin'] if isinstance(o['fin'], datetime) else (datetime.strptime(str(o['fin']), '%Y-%m-%d %H:%M:%S') if o['fin'] else ahora)
                    if (ahora - fecha_cierre) < timedelta(hours=24):
                        finalizados.append(o)
                except:
                    finalizados.append(o)
            elif es_alistado:
                o['estado_visual'] = 'ALISTADO'
                o['color_fila'] = '#eff6ff'
                activos.append(o)
            else:
                o['estado_visual'] = 'EN_PROCESO'
                if o['velocidad'] > 0 and o['velocidad'] < 1.0: 
                    o['color_fila'] = '#fef2f2'
                elif o['velocidad'] >= 1.0:
                    o['color_fila'] = '#fefce8'
                else:
                    o['color_fila'] = '#ffffff'
                activos.append(o)

        kpis['pedidos_pendientes_reales'] = len(ordenes_sin_asignar) + len(activos)
        if total_mins_speed > 0:
            kpis['velocidad_promedio'] = round(total_items_speed / total_mins_speed, 2)

        activos.sort(key=lambda x: (x['secuencia'] or 99999, x['inicio'] or datetime.min), reverse=False)
        finalizados.sort(key=lambda x: x['fin'] or datetime.min, reverse=True)
        ordenes_procesadas = activos + finalizados
        
        cur.close()

    except Exception as e:
        print(f"Error cargando dashboard: {e}")

    return render_template('B_modulo_controlador_bodegas.html', 
                           kpis=kpis, 
                           ordenes_pendientes=ordenes_sin_asignar, 
                           operarios_marcas=operarios_marcas,
                           verificadores_kpis=verificadores_kpis,
                           marcas_huerfanas=marcas_huerfanas,
                           ordenes_asignadas=ordenes_procesadas,
                           alertas_criticas=alertas_criticas)

# ==============================================================================
# ENDPOINT DE MONITOREO EN TIEMPO REAL
# ==============================================================================
@bp_bodegas.route('/api/bodegas/monitoreo_realtime')
def monitoreo_realtime():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'})
    
    empresa_id = session.get('empresa_id')
    
    kpis = {
        'pedidos_totales': 0, 
        'items_pendientes': 0, 
        'items_finalizados': 0, 
        'pedidos_pendientes_reales': 0,
        'velocidad_promedio': 0.0,
        'requiere_alias': 0,
        'backlog_verificacion': 0
    }
    
    operarios_marcas = []
    verificadores_kpis = []

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        cur.execute("""
            SELECT 
                COUNT(DISTINCT numero_orden_origen) as total,
                SUM(CASE WHEN estado_actividad NOT IN ('VERIFICADO', 'DESPACHADO', 'TERMINADO') OR estado_actividad IS NULL THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado_actividad IN ('VERIFICADO', 'DESPACHADO') THEN 1 ELSE 0 END) as listos
            FROM picking_importacion_raw 
            WHERE id_empresa = %s AND (estado_actividad != 'TERMINADO' OR estado_actividad IS NULL)
        """, (empresa_id,))
        row = cur.fetchone()
        if row:
            kpis['pedidos_totales'] = row['total'] or 0
            kpis['items_pendientes'] = int(row['pendientes'] or 0)
            kpis['items_finalizados'] = int(row['listos'] or 0)
            
        # Alias Exception
        cur.execute("""
            SELECT COUNT(*) as requiere_alias 
            FROM picking_importacion_raw 
            WHERE id_empresa = %s AND autorizacion_alistamiento = FALSE AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL)
        """, (empresa_id,))
        row_alias = cur.fetchone()
        if row_alias:
            kpis['requiere_alias'] = int(row_alias['requiere_alias'])

        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                MAX(puerta_asignada) as puerta_asignada,
                MAX(zona) as zona,
                MAX(secuencia_alistamiento) as secuencia,
                COUNT(*) as total_items,
                MAX(fecha_carga) as fecha
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND puerta_asignada IS NULL AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL) AND autorizacion_alistamiento = TRUE
            GROUP BY numero_orden_origen
            ORDER BY MAX(secuencia_alistamiento) ASC, MAX(fecha_carga) DESC
        """, (empresa_id,))
        ordenes_sin_asignar = list(cur.fetchall())
        for o in ordenes_sin_asignar:
            if isinstance(o['fecha'], datetime): o['fecha'] = o['fecha'].strftime('%Y-%m-%d %H:%M:%S')

        # KPIs Operarios
        cur.execute("""
            SELECT 
                u.id as id_operario,
                u.nombre as nombre_operario,
                GROUP_CONCAT(DISTINCT fp.marca SEPARATOR ', ') as marcas_asignadas,
                COUNT(DISTINCT fp.marca) as total_marcas,
                COUNT(p.id) as total_asignados,
                SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_completados,
                SUM(CASE WHEN p.novedad_alistamiento IS NOT NULL THEN 1 ELSE 0 END) as novedades,
                SUM(CASE WHEN p.fecha_inicio_alistamiento IS NOT NULL AND p.fecha_fin_alistamiento IS NOT NULL 
                         THEN TIMESTAMPDIFF(MINUTE, p.fecha_inicio_alistamiento, p.fecha_fin_alistamiento) ELSE 0 END) as minutos_totales
            FROM usuarios u
            LEFT JOIN fabricantes_proveedores fp ON u.id = fp.operador_asignado AND fp.id_empresa = %s
            LEFT JOIN picking_importacion_raw p ON p.marca = fp.marca AND p.id_empresa = %s AND p.puerta_asignada IS NOT NULL AND (p.estado_actividad != 'TERMINADO' OR p.estado_actividad IS NULL) AND p.autorizacion_alistamiento = TRUE
            WHERE u.empresa_id = %s AND u.perfil = 'operador_logistica'
            GROUP BY u.id, u.nombre
            ORDER BY u.nombre ASC
        """, (empresa_id, empresa_id, empresa_id))
        operarios_marcas_raw = cur.fetchall()

        for op in operarios_marcas_raw:
            op_dict = dict(op)
            asignados = int(op_dict['total_asignados'] or 0)
            completados = int(op_dict['items_completados'] or 0)
            novedades = int(op_dict['novedades'] or 0)
            mins = float(op_dict['minutos_totales'] or 0)
            
            op_dict['avance_porcentaje'] = round((completados / asignados) * 100) if asignados > 0 else 0
            op_dict['velocidad'] = round(completados / mins, 2) if mins > 0 else 0.0
            op_dict['indice_novedades'] = round((novedades / completados) * 100, 1) if completados > 0 else 0.0
            
            operarios_marcas.append(op_dict)

        # KPIs Verificadores
        cur.execute("""
            SELECT 
                u.id as id_verificador,
                u.nombre as nombre_verificador,
                COUNT(p.id) as items_verificados,
                SUM(CASE WHEN p.cajas_alistadas != p.cajas_verificadas OR p.unidades_alistadas != p.unidades_verificadas THEN 1 ELSE 0 END) as discrepancias
            FROM usuarios u
            LEFT JOIN picking_importacion_raw p ON p.id_verificador = u.id AND p.id_empresa = %s AND DATE(p.fecha_verificacion) = CURDATE()
            WHERE u.empresa_id = %s AND u.perfil IN ('verificador', 'supervisor', 'controlador_logistica')
            GROUP BY u.id, u.nombre
            HAVING items_verificados > 0
            ORDER BY items_verificados DESC
        """, (empresa_id, empresa_id))
        verificadores_raw = cur.fetchall()

        for v in verificadores_raw:
            v_dict = dict(v)
            verificados = int(v_dict['items_verificados'] or 0)
            discrepancias = int(v_dict['discrepancias'] or 0)
            v_dict['indice_discrepancia'] = round((discrepancias / verificados) * 100, 1) if verificados > 0 else 0.0
            verificadores_kpis.append(v_dict)

        # Backlog Verificación
        cur.execute("""
            SELECT COUNT(id) as total_backlog 
            FROM picking_importacion_raw 
            WHERE id_empresa = %s AND estado_actividad = 'ALISTADO' AND autorizacion_alistamiento = TRUE
        """, (empresa_id,))
        row_backlog = cur.fetchone()
        if row_backlog:
            kpis['backlog_verificacion'] = int(row_backlog['total_backlog'])

        cur.execute("""
            SELECT p.marca, COUNT(*) as total_items
            FROM picking_importacion_raw p
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa = %s AND fp.operador_asignado IS NULL AND (p.estado_actividad = 'PENDIENTE' OR p.estado_actividad IS NULL) AND p.autorizacion_alistamiento = TRUE
            GROUP BY p.marca
            ORDER BY total_items DESC
        """, (empresa_id,))
        marcas_huerfanas = list(cur.fetchall())

        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                MAX(puerta_asignada) as puerta_asignada,
                MAX(id_vehiculo) as id_vehiculo,
                MAX(zona) as zona,
                MAX(secuencia_alistamiento) as secuencia,
                COUNT(*) as total_items,
                SUM(CASE WHEN estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_listos,
                SUM(CASE WHEN estado_actividad IN ('VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_verificados,
                SUM(CASE WHEN estado_actividad IN ('DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_despachados,
                MIN(fecha_inicio_alistamiento) as inicio,
                MAX(fecha_fin_alistamiento) as fin,
                SUM(CASE WHEN fecha_inicio_alistamiento IS NOT NULL AND fecha_fin_alistamiento IS NOT NULL THEN TIMESTAMPDIFF(MINUTE, fecha_inicio_alistamiento, fecha_fin_alistamiento) ELSE 0 END) as minutos_totales
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND puerta_asignada IS NOT NULL AND autorizacion_alistamiento = TRUE
            GROUP BY numero_orden_origen
            HAVING SUM(CASE WHEN estado_actividad IN ('ASIGNADO', 'EN_PROCESO', 'ALISTADO', 'VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) > 0
        """, (empresa_id,))
        raw_ordenes = list(cur.fetchall())
        
        activos = []
        finalizados = []
        ahora = datetime.now()
        
        total_items_speed = 0
        total_mins_speed = 0

        for o in raw_ordenes:
            total_it = int(o['total_items'] or 0)
            it_listos = int(o['items_listos'] or 0)
            it_verif = int(o['items_verificados'] or 0)
            it_desp = int(o['items_despachados'] or 0)

            es_alistado = (it_listos == total_it) and (total_it > 0)
            es_verificado = (it_verif == total_it) and (total_it > 0)
            es_despachado = (it_desp == total_it) and (total_it > 0)
            
            o['velocidad'] = 0.0
            mins = float(o['minutos_totales'] or 0)
            if mins > 0:
                o['velocidad'] = round(float(it_listos) / mins, 2)
                total_items_speed += float(it_listos)
                total_mins_speed += mins
            elif float(it_listos) > 0 and o['inicio']:
                try:
                    inicio_dt = o['inicio'] if isinstance(o['inicio'], datetime) else datetime.strptime(str(o['inicio']), '%Y-%m-%d %H:%M:%S')
                    elapsed = (ahora - inicio_dt).total_seconds() / 60.0
                    if elapsed > 0:
                        o['velocidad'] = round(float(it_listos) / elapsed, 2)
                        total_items_speed += float(it_listos)
                        total_mins_speed += elapsed
                except Exception:
                    pass

            o['duracion_str'] = "--:--"
            if es_alistado and o['inicio'] and o['fin']:
                try:
                    inicio_dt = o['inicio'] if isinstance(o['inicio'], datetime) else datetime.strptime(str(o['inicio']), '%Y-%m-%d %H:%M:%S')
                    fin_dt = o['fin'] if isinstance(o['fin'], datetime) else datetime.strptime(str(o['fin']), '%Y-%m-%d %H:%M:%S')
                    diff = fin_dt - inicio_dt
                    horas = int(diff.total_seconds()) // 3600
                    minutos = (int(diff.total_seconds()) % 3600) // 60
                    segundos = int(diff.total_seconds()) % 60
                    o['duracion_str'] = f"{horas:02}:{minutos:02}:{segundos:02}"
                except Exception:
                    pass

            if es_despachado:
                o['estado_visual'] = 'DESPACHADO'
                o['color_fila'] = '#f0fdf4'
                try:
                    fecha_cierre = o['fin'] if isinstance(o['fin'], datetime) else (datetime.strptime(str(o['fin']), '%Y-%m-%d %H:%M:%S') if o['fin'] else ahora)
                    if (ahora - fecha_cierre) < timedelta(hours=24):
                        finalizados.append(o)
                except:
                    finalizados.append(o)
            elif es_verificado:
                o['estado_visual'] = 'VERIFICADO'
                o['color_fila'] = '#f0fdf4'
                try:
                    fecha_cierre = o['fin'] if isinstance(o['fin'], datetime) else (datetime.strptime(str(o['fin']), '%Y-%m-%d %H:%M:%S') if o['fin'] else ahora)
                    if (ahora - fecha_cierre) < timedelta(hours=24):
                        finalizados.append(o)
                except:
                    finalizados.append(o)
            elif es_alistado:
                o['estado_visual'] = 'ALISTADO'
                o['color_fila'] = '#eff6ff'
                activos.append(o)
            else:
                o['estado_visual'] = 'EN_PROCESO'
                if o['velocidad'] > 0 and o['velocidad'] < 1.0: 
                    o['color_fila'] = '#fef2f2'
                elif o['velocidad'] >= 1.0:
                    o['color_fila'] = '#fefce8'
                else:
                    o['color_fila'] = '#ffffff'
                activos.append(o)

            if o['inicio']:
                o['inicio_str'] = o['inicio'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(o['inicio'], datetime) else str(o['inicio'])
            else:
                o['inicio_str'] = None

            o['total_items'] = total_it
            o['items_listos'] = it_listos
            o.pop('inicio', None)
            o.pop('fin', None)

        kpis['pedidos_pendientes_reales'] = len(ordenes_sin_asignar) + len(activos)
        if total_mins_speed > 0:
            kpis['velocidad_promedio'] = round(total_items_speed / total_mins_speed, 2)

        activos.sort(key=lambda x: (x['secuencia'] or 99999, x['inicio_str'] or ''), reverse=False)
        finalizados.sort(key=lambda x: x['duracion_str'] or '', reverse=True)
        ordenes_procesadas = activos + finalizados
        
        cur.close()

        return jsonify({
            'kpis': kpis,
            'ordenes_pendientes': ordenes_sin_asignar,
            'operarios_marcas': operarios_marcas,
            'verificadores_kpis': verificadores_kpis,
            'marcas_huerfanas': marcas_huerfanas,
            'ordenes_asignadas': ordenes_procesadas
        })

    except Exception as e:
        print(f"Error en realtime: {e}")
        return jsonify({'error': str(e)})


# ==============================================================================
# ALIAS Y EXCEPCIONES IA
# ==============================================================================
@bp_bodegas.route('/api/bodegas/alias/pendientes', methods=['GET'])
def alias_pendientes():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT id, descripcion_producto as descripcion_origen, ean_leido, COUNT(*) as cantidad_veces
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND autorizacion_alistamiento = FALSE AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL)
            GROUP BY descripcion_producto
            ORDER BY cantidad_veces DESC
        """, (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([])

@bp_bodegas.route('/api/bodegas/alias/catalogo', methods=['GET'])
def alias_catalogo():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT ean, producto, fabricante 
            FROM productos 
            WHERE id_empresa = %s AND ean IS NOT NULL AND ean != ''
            ORDER BY producto ASC
        """, (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([])

@bp_bodegas.route('/api/bodegas/alias/guardar', methods=['POST'])
@csrf.exempt
def alias_guardar():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
    data = request.json
    desc_origen = data.get('descripcion_origen')
    ean_correcto = data.get('ean_correcto')
    
    if not desc_origen or not ean_correcto: return jsonify({'status': 'error', 'message': 'Faltan datos'})

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        cur.execute("SELECT producto, fabricante FROM productos WHERE id_empresa = %s AND ean = %s", (session.get('empresa_id'), ean_correcto))
        prod_db = cur.fetchone()
        
        if not prod_db: return jsonify({'status': 'error', 'message': 'Producto no encontrado en catálogo'})
        
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET codigo_producto = %s, descripcion_producto = %s, marca = %s, autorizacion_alistamiento = TRUE
            WHERE id_empresa = %s AND descripcion_producto = %s AND autorizacion_alistamiento = FALSE
        """, (ean_correcto, prod_db['producto'], prod_db['fabricante'], session.get('empresa_id'), desc_origen))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Alias guardado y pedidos liberados.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@bp_bodegas.route('/api/bodegas/alias/permitir', methods=['POST'])
@csrf.exempt
def alias_permitir():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
    data = request.json
    desc_origen = data.get('descripcion_origen')
    
    if not desc_origen: return jsonify({'status': 'error', 'message': 'Faltan datos'})

    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET autorizacion_alistamiento = TRUE
            WHERE id_empresa = %s AND descripcion_producto = %s AND autorizacion_alistamiento = FALSE
        """, (session.get('empresa_id'), desc_origen))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Bypass exitoso. Item liberado.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


@bp_bodegas.route('/api/bodegas/resolver_promo_huerfana', methods=['POST'])
@csrf.exempt
def resolver_promo_huerfana():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
    try:
        data = request.get_json()
        id_row = data.get('id_row')
        empresa_id = session.get('empresa_id')

        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        cur.execute("SELECT * FROM picking_importacion_raw WHERE id = %s AND id_empresa = %s AND estado_actividad = 'PROMO_HUERFANA'", (id_row, empresa_id))
        orphan = cur.fetchone()
        if not orphan:
            return jsonify({'status': 'error', 'message': 'No se encontró la promoción huérfana en la orden.'})

        ean_promo = orphan['codigo_producto']
        cur.execute("SELECT * FROM promociones_clientes WHERE ean_promo = %s AND id_empresa = %s AND estado = 'ACTIVO'", (ean_promo, empresa_id))
        componentes = cur.fetchall()

        if not componentes:
            cur.execute("SELECT * FROM promociones_clientes WHERE nombre_promo = %s AND id_empresa = %s AND estado = 'ACTIVO'", (orphan['descripcion_producto'], empresa_id))
            componentes = cur.fetchall()
            if componentes: ean_promo = componentes[0]['ean_promo']

        if not componentes:
            return jsonify({'status': 'error', 'message': 'El combo aún no existe en el módulo de Promociones. Configúralo primero y luego presiona este botón.'})

        nombre_promo = componentes[0]['nombre_promo']
        
        cur.execute("SELECT factor_conversion FROM productos WHERE (ean = %s OR sku = %s) AND id_empresa = %s", (ean_promo, ean_promo, empresa_id))
        prod_info = cur.fetchone()
        factor = prod_info['factor_conversion'] if prod_info and prod_info['factor_conversion'] else 1
        
        total_promos_pedidas = (orphan['cajas_calculadas'] * factor) + orphan['unidades_calculadas']
        
        data_to_insert = []
        for comp in componentes:
            hijo_code = comp['ean_componente']
            hijo_cajas_total = total_promos_pedidas * comp['cajas_componente']
            hijo_unid_total = total_promos_pedidas * comp['fracciones_componente']
            
            cur.execute("SELECT producto, fabricante FROM productos WHERE (ean = %s OR sku = %s) AND id_empresa = %s", (hijo_code, hijo_code, empresa_id))
            hijo_info = cur.fetchone()
            hijo_desc = hijo_info['producto'] if hijo_info else f"ITEM SIN NOMBRE ({hijo_code})"
            hijo_marca = hijo_info['fabricante'] if hijo_info else 'GENERICO'
            
            hijo_desc_visual = f"{hijo_desc} (Kit: {nombre_promo})"
            
            if hijo_cajas_total > 0 or hijo_unid_total > 0:
                data_to_insert.append((
                    empresa_id, orphan['numero_orden_origen'], orphan['zona'], hijo_code, orphan['ean_leido'], 
                    hijo_desc_visual, hijo_marca, hijo_cajas_total, 0, hijo_unid_total, 0, 'PENDIENTE', 
                    orphan['fecha_creacion_orden'], orphan['fecha_entrega_orden'], True
                ))

        if data_to_insert:
            query = """INSERT INTO picking_importacion_raw 
            (id_empresa, numero_orden_origen, zona, codigo_producto, ean_leido, descripcion_producto, marca, cajas_calculadas, cajas_alistadas, unidades_calculadas, unidades_alistadas, estado_actividad, fecha_creacion_orden, fecha_entrega_orden, autorizacion_alistamiento) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cur.executemany(query, data_to_insert)

        cur.execute("DELETE FROM picking_importacion_raw WHERE id = %s", (id_row,))
        mysql.connection.commit()
        cur.close()

        return jsonify({'status': 'success', 'message': 'Promoción resuelta. Los componentes han sido liberados a la bodega.'})
    
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)})

# ==============================================================================
# REPORTES Y EXPORTACIONES
# ==============================================================================


@bp_bodegas.route('/api/bodegas/reportes/novedades')
def reporte_novedades():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                p.numero_orden_origen as orden, p.codigo_producto, p.descripcion_producto,
                p.novedad_alistamiento, p.nombre_auxiliar_asignado as operario,
                IFNULL(s.nombre, 'Sin Auth') as supervisor, p.fecha_fin_alistamiento as fecha
            FROM picking_importacion_raw p
            LEFT JOIN usuarios s ON p.id_supervisor_novedad = s.id
            WHERE p.id_empresa = %s AND p.novedad_alistamiento IS NOT NULL
            ORDER BY p.fecha_fin_alistamiento DESC LIMIT 100
        """, (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except: return jsonify([])

@bp_bodegas.route('/api/bodegas/historial_actas')
def historial_actas():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                a.numero_orden, 
                a.placa_vehiculo, 
                IFNULL(u.nombre, 'Sin Auth') as supervisor, 
                a.fecha_generacion
            FROM actas_despacho_flotacarga a
            LEFT JOIN usuarios u ON a.id_supervisor_despacho = u.id
            WHERE a.id_empresa = %s
            ORDER BY a.fecha_generacion DESC LIMIT 100
        """, (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        
        for row in data:
            if isinstance(row['fecha_generacion'], datetime):
                row['fecha_generacion'] = row['fecha_generacion'].strftime('%Y-%m-%d %H:%M:%S')
                
        return jsonify(data)
    except Exception as e:
        print(f"Error historial actas: {e}")
        return jsonify([])

# ==============================================================================
# RESTO DE APIS MULTIEMPRESA (MANTENIDAS AL 100%)
# ==============================================================================

@bp_bodegas.route('/api/bodegas/operarios_asignacion', methods=['GET'])
def operarios_asignacion():
    if 'usuario_id' not in session: return jsonify({})
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        cur.execute("SELECT id, nombre FROM usuarios WHERE empresa_id = %s AND perfil = 'operador_logistica' ORDER BY nombre ASC", (empresa_id,))
        operarios = cur.fetchall()

        # UPPER() añadido para prevenir errores de case sensitivity en el cruce 
        cur.execute("""
            SELECT DISTINCT m.marca, m.nombre_marca, fp.operador_asignado
            FROM (
                SELECT UPPER(fabricante) as marca, UPPER(fabricante) as nombre_marca FROM productos WHERE id_empresa = %s AND fabricante IS NOT NULL AND fabricante != ''
                UNION
                SELECT UPPER(marca) as marca, UPPER(marca) as nombre_marca FROM picking_importacion_raw WHERE id_empresa = %s AND marca IS NOT NULL AND marca != ''
            ) m
            LEFT JOIN fabricantes_proveedores fp ON m.marca = fp.marca AND fp.id_empresa = %s
            ORDER BY m.nombre_marca ASC
        """, (empresa_id, empresa_id, empresa_id))
        marcas = cur.fetchall()

        cur.close()
        return jsonify({'operarios': operarios, 'marcas': marcas})
    except Exception as e:
        return jsonify({'operarios': [], 'marcas': []})

@bp_bodegas.route('/api/bodegas/guardar_asignacion_marcas', methods=['POST'])
@csrf.exempt
def guardar_asignacion_marcas():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'}), 401
    d = request.json
    empresa_id = session.get('empresa_id')
    id_operario = d.get('id_operario')
    marcas = d.get('marcas', []) 
    
    if not id_operario: return jsonify({'status': 'error', 'message': 'Faltan datos.'})

    try:
        cur = mysql.connection.cursor()
        cur.execute("DELETE FROM fabricantes_proveedores WHERE id_empresa = %s AND operador_asignado = %s", (empresa_id, id_operario))
        
        if marcas:
            data_to_insert = [(empresa_id, str(m).upper(), id_operario) for m in marcas]
            cur.executemany("""
                INSERT INTO fabricantes_proveedores (id_empresa, marca, operador_asignado)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE operador_asignado = VALUES(operador_asignado)
            """, data_to_insert)
            
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Asignación guardada correctamente.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@bp_bodegas.route('/bodegas/asignar_puerta', methods=['POST'])
@csrf.exempt
def asignar_puerta():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión'}), 401
    d = request.json
    try:
        cur = mysql.connection.cursor()
        secuencia = d.get('secuencia', 0)
        zona = d.get('zona', 'GENERAL')
        
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET puerta_asignada=%s, secuencia_alistamiento=%s, zona=%s, estado_actividad='ASIGNADO', fecha_inicio_alistamiento=NOW()
            WHERE numero_orden_origen=%s AND id_empresa=%s
        """, (d['puerta'], secuencia, zona, d['numero_orden'], session.get('empresa_id')))
            
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': 'Muelle y configuración asignada correctamente.'})
    except Exception as e: return jsonify({'error': str(e)}), 500

@bp_bodegas.route('/bodegas/api/items_orden/<orden>')
def get_items_orden(orden):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, p.marca, 
                p.cajas_calculadas, p.cajas_alistadas, p.cajas_verificadas,
                p.unidades_calculadas, p.unidades_alistadas, p.unidades_verificadas,
                p.estado_actividad,
                IFNULL(prod.unidad_embalaje, 'UND') as embalaje
            FROM picking_importacion_raw p
            LEFT JOIN productos prod 
                ON (p.codigo_producto = prod.ean OR p.codigo_producto = prod.sku) 
                AND p.id_empresa = prod.id_empresa
            WHERE p.numero_orden_origen = %s AND p.id_empresa = %s
        """, (orden, session.get('empresa_id')))
        items = cur.fetchall()
        cur.close()
        return jsonify(items)
    except Exception as e:
        return jsonify([])
    
@bp_bodegas.route('/bodegas/api/get_empleados')
def get_empleados():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT id, nombre 
            FROM usuarios 
            WHERE empresa_id = %s AND perfil = 'operador_logistica'
        """, (session.get('empresa_id'),))
        return jsonify(cur.fetchall())
    except: return jsonify([])

@bp_bodegas.route('/api/bodegas/stats')
def bodegas_stats():
    if 'usuario_id' not in session: return jsonify({})
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor()
    cur.execute("SELECT COUNT(DISTINCT numero_orden_origen), SUM(CASE WHEN estado_actividad='PENDIENTE' OR estado_actividad IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN estado_actividad IN ('VERIFICADO','DESPACHADO') THEN 1 ELSE 0 END) FROM picking_importacion_raw WHERE id_empresa = %s AND (estado_actividad != 'TERMINADO' OR estado_actividad IS NULL)", (empresa_id,))
    row = cur.fetchone()
    cur.close()
    return jsonify({'ordenes_activas': row[0] or 0, 'items_pendientes': int(row[1] or 0), 'items_finalizados': int(row[2] or 0)})

@bp_bodegas.route('/api/bodegas/productos/crear', methods=['POST']) 
@csrf.exempt
def crear_producto_manual():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    
    data = request.json
    nit_empresa = str(session.get('empresa_id', ''))
    nombre_empresa = str(session.get('nombre_empresa', ''))
    
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT tipo_empresa FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
        res = cur.fetchone()
        tipo_empresa = res[0] if res else 'general'

        cur.execute("""
            INSERT INTO productos (id_empresa, empresa, tipo_empresa, sku, ean, producto, fabricante, unidad_embalaje)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            producto = VALUES(producto), 
            fabricante = VALUES(fabricante), 
            unidad_embalaje = VALUES(unidad_embalaje)
        """, (nit_empresa, nombre_empresa, tipo_empresa, data.get('ean'), data.get('ean'), data.get('producto'), data.get('fabricante'), data.get('unidad_embalaje', 'UND')))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
        
@bp_bodegas.route('/bodegas/descargar_plantilla')
def descargar_plantilla_productos():
    if 'usuario_id' not in session: return redirect('/')
    nit_sesion = str(session.get('empresa_id', ''))
    
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT nombre_comercial, tipo_empresa FROM empresas WHERE nit = %s", (nit_sesion,))
        emp_data = cur.fetchone()
        cur.close()
        
        if emp_data:
            nombre_real = emp_data[0]
            tipo_real = emp_data[1]
        else:
            cur = mysql.connection.cursor()
            cur.execute("SELECT nombre_comercial, tipo_empresa FROM empresas WHERE id = %s", (nit_sesion,))
            emp_data = cur.fetchone()
            cur.close()
            nombre_real = emp_data[0] if emp_data else "Empresa no encontrada"
            tipo_real = emp_data[1] if emp_data else "general"
    except Exception as e:
        nombre_real = "Error de Conexión"
        tipo_real = "general"

    df_plantilla = pd.DataFrame([{
        'ID_EMPRESA': nit_sesion,
        'EMPRESA': nombre_real,
        'TIPO_EMPRESA': tipo_real,
        'FABRICANTE': 'MARCA',
        'PRODUCTO': 'DESCRIPCIÓN',
        'EAN': '0000000000000',
        'UNIDAD_EMBALAJE': 'UND',
        'FACTOR_CONVERSION': 1
    }])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_plantilla.to_excel(writer, index=False, sheet_name='Plantilla')
    output.seek(0)

    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True, download_name='Plantilla_Productos.xlsx')
    
@bp_bodegas.route('/bodegas/upload_productos_masivo', methods=['POST'])
@csrf.exempt
def upload_productos_masivo():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    file = request.files.get('file')
    if not file: return jsonify({'error': 'No hay archivo'}), 400

    try:
        if file.filename.endswith('.xlsx'):
            df = pd.read_excel(file, dtype=str)
        else:
            df = pd.read_csv(file, dtype=str, sep=None, engine='python')
            
        df.columns = df.columns.str.strip().str.upper()
        empresa_id = str(session.get('empresa_id'))
        
        cur = mysql.connection.cursor()
        cur.execute("SELECT nombre_comercial, tipo_empresa FROM empresas WHERE nit = %s OR id = %s", (empresa_id, empresa_id))
        emp_data = cur.fetchone()
        
        nombre_empresa = emp_data[0] if emp_data else 'Empresa'
        tipo_empresa = emp_data[1] if emp_data else 'general'

        data_to_upsert = []
        for _, row in df.iterrows():
            ean = str(row.get('EAN', '')).strip()
            producto = str(row.get('PRODUCTO', '')).strip()
            fabricante = str(row.get('FABRICANTE', '')).strip()
            embalaje = str(row.get('UNIDAD_EMBALAJE', 'UND')).strip()
            
            if not ean or not producto or ean.upper() == 'NAN':
                continue

            data_to_upsert.append((
                empresa_id, nombre_empresa, tipo_empresa, ean, ean, 
                producto, fabricante, embalaje
            ))

        if data_to_upsert:
            cur.executemany("""
                INSERT INTO productos (id_empresa, empresa, tipo_empresa, sku, ean, producto, fabricante, unidad_embalaje)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                empresa=VALUES(empresa), 
                tipo_empresa=VALUES(tipo_empresa), 
                producto=VALUES(producto),
                fabricante=VALUES(fabricante),
                unidad_embalaje=VALUES(unidad_embalaje)
            """, data_to_upsert)
            mysql.connection.commit()
        
        cur.close()
        return jsonify({'message': f'✅ {len(data_to_upsert)} productos procesados y guardados correctamente.'})
    
    except Exception as e:
        return jsonify({'error': f'Error en el archivo: {str(e)}'}), 500

@bp_bodegas.route('/api/bodegas/marcas', methods=['GET'])
def get_marcas():
    if 'usuario_id' not in session: return jsonify([])
    try:
        empresa_id = session.get('empresa_id')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT DISTINCT IF(fabricante IS NULL OR fabricante = '', 'SIN MARCA', UPPER(fabricante)) as fabricante 
            FROM productos 
            WHERE id_empresa = %s
            ORDER BY fabricante ASC
        """, (empresa_id,))
        marcas = cur.fetchall()
        cur.close()
        return jsonify(marcas)
    except Exception as e: return jsonify([])
    
@bp_bodegas.route('/api/bodegas/productos_por_marca/<marca>', methods=['GET'])
def get_productos_por_marca(marca):
    if 'usuario_id' not in session: return jsonify([])
    try:
        empresa_id = session.get('empresa_id')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        if marca == 'SIN MARCA':
            cur.execute("""
                SELECT ean, producto, fabricante, unidad_embalaje 
                FROM productos 
                WHERE id_empresa = %s AND (fabricante IS NULL OR fabricante = '')
                ORDER BY producto ASC
            """, (empresa_id,))
        else:
            cur.execute("""
                SELECT ean, producto, fabricante, unidad_embalaje 
                FROM productos 
                WHERE id_empresa = %s AND UPPER(fabricante) = UPPER(%s) 
                ORDER BY producto ASC
            """, (empresa_id, marca))
            
        productos = cur.fetchall()
        cur.close()
        return jsonify(productos)
    except Exception as e: return jsonify([])

@bp_bodegas.route('/api/bodegas/editar_producto', methods=['POST'])
def editar_producto():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
        
    try:
        empresa_id = session.get('empresa_id')
        data = request.get_json()
        ean = data.get('ean')
        nuevo_nombre = data.get('producto')
        nuevo_embalaje = data.get('unidad_embalaje')
        
        if not ean or not nuevo_nombre: return jsonify({'status': 'error', 'message': 'Faltan datos'})

        cur = mysql.connection.cursor()
        cur.execute("UPDATE productos SET producto = %s, unidad_embalaje = %s WHERE ean = %s AND id_empresa = %s", (nuevo_nombre, nuevo_embalaje, ean, empresa_id))
        mysql.connection.commit()
        cur.close()
        
        return jsonify({'status': 'success', 'message': 'Producto actualizado'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': 'Error interno'})

@bp_bodegas.route('/api/bodegas/eliminar_producto', methods=['POST'])
def eliminar_producto():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
        
    try:
        empresa_id = session.get('empresa_id')
        data = request.get_json()
        ean = data.get('ean')
        
        if not ean: return jsonify({'status': 'error', 'message': 'EAN obligatorio'})

        cur = mysql.connection.cursor()
        cur.execute("DELETE FROM productos WHERE ean = %s AND id_empresa = %s", (ean, empresa_id))
        
        filas_afectadas = cur.rowcount
        mysql.connection.commit()
        cur.close()
        
        if filas_afectadas > 0: return jsonify({'status': 'success', 'message': 'Producto eliminado'})
        else: return jsonify({'status': 'error', 'message': 'No se encontró'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': 'Error interno'})

@bp_bodegas.route('/api/bodegas/eliminar_marca', methods=['POST'])
def eliminar_marca():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
        
    try:
        empresa_id = session.get('empresa_id')
        data = request.get_json()
        fabricante = data.get('fabricante')
        
        if not fabricante: return jsonify({'status': 'error', 'message': 'Marca obligatoria'})

        cur = mysql.connection.cursor()
        if fabricante == 'SIN MARCA': cur.execute("DELETE FROM productos WHERE (fabricante IS NULL OR fabricante = '') AND id_empresa = %s", (empresa_id,))
        else: cur.execute("DELETE FROM productos WHERE UPPER(fabricante) = UPPER(%s) AND id_empresa = %s", (fabricante, empresa_id))
        
        filas_afectadas = cur.rowcount
        mysql.connection.commit()
        cur.close()
        
        if filas_afectadas > 0: return jsonify({'status': 'success', 'message': f'Eliminados {filas_afectadas}'})
        else: return jsonify({'status': 'error', 'message': 'No hay productos'})
            
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': 'Error interno'})

@bp_bodegas.route('/api/promociones/listar', methods=['GET'])
def listar_promociones():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT ean_promo, MAX(nombre_promo) as nombre_promo, MAX(estado) as estado,
                   COUNT(ean_componente) as total_componentes
            FROM promociones_clientes
            WHERE id_empresa = %s
            GROUP BY ean_promo
            ORDER BY created_at DESC
        """, (session.get('empresa_id'),))
        promos = cur.fetchall()
        cur.close()
        return jsonify(promos)
    except Exception as e:
        return jsonify([])

@bp_bodegas.route('/api/promociones/detalle/<ean_promo>', methods=['GET'])
def detalle_promocion(ean_promo):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT p.ean_componente, p.cajas_componente, p.fracciones_componente,
                   IFNULL(m.producto, 'Producto Desconocido') as descripcion_componente,
                   IFNULL(m.fabricante, 'N/A') as marca_componente
            FROM promociones_clientes p
            LEFT JOIN productos m ON (p.ean_componente = m.ean OR p.ean_componente = m.sku) AND p.id_empresa = m.id_empresa
            WHERE p.ean_promo = %s AND p.id_empresa = %s
        """, (ean_promo, session.get('empresa_id')))
        detalle = cur.fetchall()
        cur.close()
        return jsonify(detalle)
    except Exception as e:
        return jsonify([])

@bp_bodegas.route('/api/promociones/guardar', methods=['POST'])
def guardar_promocion():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
    try:
        data = request.json
        empresa_id = session.get('empresa_id')
        nombre_empresa = session.get('nombre_empresa', 'Empresa')
        ean_promo = data.get('ean_promo')
        nombre_promo = data.get('nombre_promo')
        componentes = data.get('componentes', [])

        if not ean_promo or not componentes:
            return jsonify({'status': 'error', 'message': 'Faltan datos'})

        cur = mysql.connection.cursor()
        cur.execute("DELETE FROM promociones_clientes WHERE ean_promo = %s AND id_empresa = %s", (ean_promo, empresa_id))
        
        data_to_insert = []
        for comp in componentes:
            data_to_insert.append((
                nombre_empresa, empresa_id, ean_promo, nombre_promo,
                comp['ean'], comp['cajas'], comp['unidades'], 'ACTIVO'
            ))
        
        if data_to_insert:
            cur.executemany("""
                INSERT INTO promociones_clientes 
                (empresa, id_empresa, ean_promo, nombre_promo, ean_componente, cajas_componente, fracciones_componente, estado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, data_to_insert)
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Promoción guardada'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': 'Error al guardar'})

@bp_bodegas.route('/api/promociones/estado', methods=['POST'])
def cambiar_estado_promocion():
    if 'usuario_id' not in session: return jsonify({'status': 'error'})
    try:
        data = request.json
        cur = mysql.connection.cursor()
        cur.execute("UPDATE promociones_clientes SET estado = %s WHERE ean_promo = %s AND id_empresa = %s", (data['estado'], data['ean_promo'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success'})
    except:
        mysql.connection.rollback()
        return jsonify({'status': 'error'})

@bp_bodegas.route('/api/promociones/eliminar', methods=['POST'])
def eliminar_promocion():
    if 'usuario_id' not in session: return jsonify({'status': 'error'})
    try:
        data = request.json
        cur = mysql.connection.cursor()
        cur.execute("DELETE FROM promociones_clientes WHERE ean_promo = %s AND id_empresa = %s", (data['ean_promo'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success'})
    except:
        mysql.connection.rollback()
        return jsonify({'status': 'error'})

@bp_bodegas.route('/api/bodegas/vehiculos')
def get_vehiculos():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT id, placa FROM vehiculos WHERE id_empresa = %s", (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e: 
        return jsonify([])
    
@bp_bodegas.route('/bodegas/despachar_orden', methods=['POST'])
@csrf.exempt
def despachar_orden():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión'}), 401
    d = request.json
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        cur.execute("SELECT placa FROM vehiculos WHERE id = %s", (d['id_vehiculo'],))
        veh = cur.fetchone()
        placa = veh['placa'] if veh else 'N/A'
        
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET estado_actividad='TERMINADO', id_vehiculo=%s, fecha_despacho=NOW(), id_supervisor_despacho=%s
            WHERE numero_orden_origen=%s AND id_empresa=%s
        """, (d['id_vehiculo'], session.get('usuario_id'), d['numero_orden'], session.get('empresa_id')))
        
        cur.execute("""
            INSERT INTO actas_despacho_flotacarga 
            (id_empresa, numero_orden, placa_vehiculo, id_supervisor_despacho, foto_evidencia, firma_evidencia)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (session.get('empresa_id'), d['numero_orden'], placa, session.get('usuario_id'), d.get('foto'), d.get('firma')))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': 'Orden despachada y acta generada correctamente.'})
    except Exception as e: return jsonify({'error': str(e)}), 500

@bp_bodegas.route('/bodegas/imprimir_acta/<orden>')
def imprimir_acta(orden):
    if 'usuario_id' not in session: return redirect('/')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                p.codigo_producto, p.descripcion_producto, p.cajas_verificadas, p.unidades_verificadas,
                p.nombre_auxiliar_asignado, uv.nombre as nombre_verificador_asignado, p.puerta_asignada, p.fecha_despacho, p.novedad_alistamiento,
                v.placa, u.nombre as supervisor, sup_nov.nombre as nombre_supervisor_novedad
            FROM picking_importacion_raw p
            LEFT JOIN vehiculos v ON p.id_vehiculo = v.id
            LEFT JOIN usuarios u ON p.id_supervisor_despacho = u.id
            LEFT JOIN usuarios sup_nov ON p.id_supervisor_novedad = sup_nov.id
            LEFT JOIN usuarios uv ON p.id_verificador = uv.id
            WHERE p.numero_orden_origen = %s AND p.id_empresa = %s AND p.estado_actividad IN ('VERIFICADO', 'DESPACHADO', 'TERMINADO')
        """, (orden, session.get('empresa_id')))
        items = cur.fetchall()
        
        cur.execute("""
            SELECT foto_evidencia, firma_evidencia 
            FROM actas_despacho_flotacarga 
            WHERE numero_orden = %s AND id_empresa = %s
            ORDER BY id DESC LIMIT 1
        """, (orden, session.get('empresa_id')))
        acta_info = cur.fetchone()
        cur.close()
        
        if not items: return "Orden no encontrada o no verificada.", 404
            
        head = items[0]
        items_normales = [i for i in items if i['novedad_alistamiento'] is None and (i['cajas_verificadas']>0 or i['unidades_verificadas']>0)]
        items_novedad = [i for i in items if i['novedad_alistamiento'] is not None or (i['cajas_verificadas']==0 and i['unidades_verificadas']==0)]
        
        firma_img = f"<img src='{acta_info['firma_evidencia']}' />" if acta_info and acta_info['firma_evidencia'] else ""
        foto_img = f"<img src='{acta_info['foto_evidencia']}' style='max-width:100%; max-height:300px; display:block; margin: 0 auto; border-radius: 8px;'/>" if acta_info and acta_info['foto_evidencia'] else ""
        
        html = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="UTF-8">
            <title>Acta de Entrega - {orden}</title>
            <style>
                body {{ font-family: Arial, sans-serif; padding: 40px; color: #333; }}
                h1, h2, h3 {{ color: #000; text-align: center; margin: 5px 0; }}
                .header-box {{ border: 2px solid #000; padding: 20px; border-radius: 10px; margin-bottom: 30px; }}
                .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-top: 15px; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; margin-bottom: 30px; }}
                th, td {{ border: 1px solid #000; padding: 10px; text-align: left; font-size:14px; }}
                th {{ background: #f0f0f0; }}
                .firmas {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 40px; margin-top: 80px; page-break-inside: avoid; }}
                .firma-col {{ display: flex; flex-direction: column; justify-content: flex-end; align-items: center; }}
                .firma-espacio {{ height: 100px; display: flex; align-items: flex-end; justify-content: center; margin-bottom: 5px; width: 100%; }}
                .firma-espacio img {{ max-height: 100px; max-width: 100%; object-fit: contain; }}
                .firma-linea {{ border-top: 2px solid #000; width: 100%; text-align: center; padding-top: 10px; font-size:15px; }}
                .btn-print {{ display: block; margin: 0 auto 30px auto; padding: 15px 30px; background: #004e92; color: white; border: none; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; }}
                @media print {{ .btn-print {{ display: none; }} body {{ padding: 0; }} }}
            </style>
        </head>
        <body>
            <button class="btn-print" onclick="window.print()">🖨️ Imprimir Manifiesto</button>
            <div class="header-box">
                <h1>MANIFIESTO DE CARGA Y ENTREGA (VERIFICADO)</h1>
                <h3>ORDEN DE PEDIDO: #{orden}</h3>
                <div class="info-grid">
                    <div><b>🏢 Muelle/Puerta:</b> {head.get('puerta_asignada', 'SIN PUERTA')}</div>
                    <div><b>🚚 Placa Vehículo:</b> {head.get('placa', 'No Registrado')}</div>
                    <div><b>👨‍✈️ Conductor:</b> El asignado al vehiculo</div>
                    <div><b>📅 Fecha de Despacho:</b> {head.get('fecha_despacho', 'N/A')}</div>
                    <div><b>📋 Despachador (Sup):</b> {head.get('supervisor', 'N/A')}</div>
                    <div><b>✅ Verificador:</b> {head.get('nombre_verificador_asignado', 'N/A')}</div>
                </div>
            </div>
            <h3>Mercancía Verificada Conforme</h3>
            <table>
                <thead>
                    <tr>
                        <th>Código</th><th>Producto</th><th>Alistador</th><th style="text-align:center;">Cajas (Verif)</th><th style="text-align:center;">Unidades (Verif)</th>
                    </tr>
                </thead>
                <tbody>
        """
        if items_normales:
            for item in items_normales:
                html += f"<tr><td>{item['codigo_producto'] or 'S/C'}</td><td>{item['descripcion_producto']}</td><td>{item['nombre_auxiliar_asignado'] or 'N/A'}</td><td style='text-align:center; font-weight:bold;'>{item['cajas_verificadas']}</td><td style='text-align:center; font-weight:bold;'>{item['unidades_verificadas']}</td></tr>"
        else:
            html += "<tr><td colspan='5' style='text-align:center;'>No hay mercancía conforme</td></tr>"
            
        html += """</tbody></table>"""

        if items_novedad:
            html += """
            <h3 style="color:#b00020;">Novedades y Faltantes</h3>
            <table>
                <thead style="background:#fee2e2;">
                    <tr><th style="background:#fecaca;">Producto</th><th style="background:#fecaca;">Novedad</th><th style="background:#fecaca; text-align:center;">Cajas Entregadas</th><th style="background:#fecaca; text-align:center;">Unid. Entregadas</th><th style="background:#fecaca;">Autoriza</th></tr>
                </thead>
                <tbody>
            """
            for item in items_novedad:
                if item['cajas_verificadas'] == 0 and item['unidades_verificadas'] == 0 and not item['novedad_alistamiento']:
                    novedad_texto = "ÍTEM NO ALISTADO POR FALTA DE EXISTENCIAS"
                else:
                    novedad_texto = "Falta Existencia" if item['novedad_alistamiento'] == 'FALTA_EXISTENCIAS' else "Alistado sin EAN (Verificado a ciegas)"
                
                auth = item['nombre_supervisor_novedad'] or 'Operario/Sistema'
                html += f"<tr><td>{item['descripcion_producto']} <small>({item['codigo_producto']})</small></td><td style='color:#b00020; font-weight:bold;'>{novedad_texto}</td><td style='text-align:center;'>{item['cajas_verificadas']}</td><td style='text-align:center;'>{item['unidades_verificadas']}</td><td>{auth}</td></tr>"
            html += """</tbody></table>"""
            
        html += f"""
            <div class="firmas">
                <div class="firma-col">
                    <div class="firma-espacio"></div>
                    <div class="firma-linea"><b>Verificador</b><br>{head.get('nombre_verificador_asignado', 'Firma Verificador')}</div>
                </div>
                <div class="firma-col">
                    <div class="firma-espacio"></div>
                    <div class="firma-linea"><b>Supervisor Despacho</b><br>{head.get('supervisor', 'Firma Responsable')}</div>
                </div>
                <div class="firma-col">
                    <div class="firma-espacio">{firma_img}</div>
                    <div class="firma-linea">
                        <b>Recibí Conforme (Conductor)</b><br>
                        El asignado al vehiculo
                    </div>
                </div>
            </div>
            <div style="margin-top: 40px; text-align:center;">
                <h4 style="color:#64748b;">Evidencia Fotográfica</h4>
                {foto_img}
            </div>
        </body>
        </html>
        """
        return html
    except Exception as e:
        return str(e), 500

@bp_bodegas.route('/bodegas/api/ordenes_sin_secuencia')
def ordenes_sin_secuencia():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                DATE_FORMAT(MAX(fecha_carga), '%%Y-%%m-%%d %%H:%%i:%%s') as fecha_carga,
                MAX(zona) as zona,
                MAX(secuencia_alistamiento) as secuencia
            FROM picking_importacion_raw
            WHERE id_empresa = %s
            GROUP BY numero_orden_origen
            ORDER BY MAX(fecha_carga) DESC
        """, (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        print(f"Error en ordenes_sin_secuencia: {e}")
        return jsonify([])
    
@bp_bodegas.route('/bodegas/guardar_secuencia_zona', methods=['POST'])
@csrf.exempt
def guardar_secuencia_zona():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    d = request.json
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET zona=%s, secuencia_alistamiento=%s
            WHERE numero_orden_origen=%s AND id_empresa=%s
        """, (d['zona'], d['secuencia'], d['orden'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': 'Zona y orden de secuencia actualizados correctamente.'})
    except Exception as e: return jsonify({'error': str(e)}), 500

@bp_bodegas.route('/api/bodegas/reportes/kpis_historico')
def kpis_historico():
    if 'usuario_id' not in session: return jsonify([])
    empresa_id = session.get('empresa_id')
    lapso = request.args.get('lapso', 'hoy')
    
    hoy = datetime.now()
    
    if lapso == 'hoy':
        fecha_inicio = hoy.date()
        fecha_fin = hoy.date()
        formato_grupo = '%Y-%m-%d %H:00'
    elif lapso == 'ayer':
        ayer = hoy - timedelta(days=1)
        fecha_inicio = ayer.date()
        fecha_fin = ayer.date()
        formato_grupo = '%Y-%m-%d %H:00'
    elif lapso == 'mes_anterior':
        primer_dia_este_mes = hoy.replace(day=1)
        ultimo_dia_mes_anterior = primer_dia_este_mes - timedelta(days=1)
        fecha_inicio = ultimo_dia_mes_anterior.replace(day=1).date()
        fecha_fin = ultimo_dia_mes_anterior.date()
        formato_grupo = '%Y-%m-%d'
    elif lapso == 'trimestre_anterior':
        trimestre_actual = (hoy.month - 1) // 3 + 1
        trimestre_anterior = trimestre_actual - 1 if trimestre_actual > 1 else 4
        ano_trimestre = hoy.year if trimestre_actual > 1 else hoy.year - 1
        mes_inicio = (trimestre_anterior - 1) * 3 + 1
        
        fecha_inicio = datetime(ano_trimestre, mes_inicio, 1).date()
        
        if trimestre_anterior == 1: mes_fin, dia_fin = 3, 31
        elif trimestre_anterior == 2: mes_fin, dia_fin = 6, 30
        elif trimestre_anterior == 3: mes_fin, dia_fin = 9, 30
        else: mes_fin, dia_fin = 12, 31
        
        fecha_fin = datetime(ano_trimestre, mes_fin, dia_fin).date()
        formato_grupo = '%Y-%m'
    else:
        fecha_inicio = hoy.date()
        fecha_fin = hoy.date()
        formato_grupo = '%Y-%m-%d'

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # 1. Global (Línea de tiempo)
        query_global = """
            SELECT 
                DATE_FORMAT(fecha_creacion_orden, %s) as etiqueta,
                COUNT(DISTINCT numero_orden_origen) as total_pedidos,
                SUM(CASE WHEN estado_actividad IN ('VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_listos,
                COUNT(*) as total_items,
                AVG(CASE WHEN fecha_inicio_alistamiento IS NOT NULL AND fecha_fin_alistamiento IS NOT NULL 
                         THEN TIMESTAMPDIFF(MINUTE, fecha_inicio_alistamiento, fecha_fin_alistamiento) ELSE NULL END) as tiempo_promedio_min
            FROM picking_importacion_raw
            WHERE id_empresa = %s 
              AND DATE(fecha_creacion_orden) BETWEEN %s AND %s
            GROUP BY etiqueta
            ORDER BY etiqueta ASC
        """
        cur.execute(query_global, (formato_grupo, empresa_id, fecha_inicio, fecha_fin))
        datos_globales = list(cur.fetchall())
        
        # 2. Operarios Histórico
        query_operarios = """
            SELECT 
                nombre_auxiliar_asignado as operario,
                COUNT(id) as items_completados,
                SUM(CASE WHEN novedad_alistamiento IS NOT NULL THEN 1 ELSE 0 END) as novedades,
                SUM(CASE WHEN fecha_inicio_alistamiento IS NOT NULL AND fecha_fin_alistamiento IS NOT NULL 
                         THEN TIMESTAMPDIFF(MINUTE, fecha_inicio_alistamiento, fecha_fin_alistamiento) ELSE 0 END) as minutos_totales
            FROM picking_importacion_raw
            WHERE id_empresa = %s 
              AND nombre_auxiliar_asignado IS NOT NULL
              AND estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'TERMINADO')
              AND DATE(fecha_fin_alistamiento) BETWEEN %s AND %s
            GROUP BY nombre_auxiliar_asignado
            ORDER BY items_completados DESC
        """
        cur.execute(query_operarios, (empresa_id, fecha_inicio, fecha_fin))
        ops_raw = cur.fetchall()
        
        operarios_kpis = []
        for op in ops_raw:
            op_d = dict(op)
            completados = int(op_d['items_completados'] or 0)
            novedades = int(op_d['novedades'] or 0)
            mins = float(op_d['minutos_totales'] or 0)
            
            op_d['velocidad'] = round(completados / mins, 2) if mins > 0 else 0.0
            op_d['indice_novedades'] = round((novedades / completados) * 100, 1) if completados > 0 else 0.0
            operarios_kpis.append(op_d)
            
        # 3. Verificadores Histórico
        query_verificadores = """
            SELECT 
                u.nombre as verificador,
                COUNT(p.id) as items_verificados,
                SUM(CASE WHEN p.cajas_alistadas != p.cajas_verificadas OR p.unidades_alistadas != p.unidades_verificadas THEN 1 ELSE 0 END) as discrepancias
            FROM picking_importacion_raw p
            INNER JOIN usuarios u ON p.id_verificador = u.id
            WHERE p.id_empresa = %s 
              AND DATE(p.fecha_verificacion) BETWEEN %s AND %s
            GROUP BY u.nombre
            ORDER BY items_verificados DESC
        """
        cur.execute(query_verificadores, (empresa_id, fecha_inicio, fecha_fin))
        verif_raw = cur.fetchall()
        
        verificadores_kpis = []
        for v in verif_raw:
            v_d = dict(v)
            verificados = int(v_d['items_verificados'] or 0)
            discrepancias = int(v_d['discrepancias'] or 0)
            v_d['indice_discrepancia'] = round((discrepancias / verificados) * 100, 1) if verificados > 0 else 0.0
            verificadores_kpis.append(v_d)

        cur.close()
        
        return jsonify({
            'global': datos_globales,
            'operarios': operarios_kpis,
            'verificadores': verificadores_kpis
        })
    except Exception as e:
        print(f"Error reportes kpis historico: {e}")
        return jsonify({'global': [], 'operarios': [], 'verificadores': []})

# ==============================================================================
# LECTURA DE EXCEL IA (ALGORITMO INTELIGENTE CORREGIDO CON REGLAS ESTRICTAS)
# ==============================================================================
def normalizar_codigo(valor):
    if pd.isna(valor) or str(valor).strip() == '': return ''
    val_str = str(valor).strip()
    if 'E' in val_str.upper():
        try: return str(int(float(valor)))
        except: pass
    if val_str.endswith('.0'): return val_str[:-2]
    return val_str

def es_cadena_numerica(valor):
    if pd.isna(valor): return False
    v = str(valor).strip()
    if v.endswith('.0'): v = v[:-2]
    return v.isdigit()

def limpiar_texto(texto):
    if pd.isna(texto) or texto is None: return ""
    texto = str(texto).upper().strip()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto = texto.replace(".", "")
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def match_keyword(celda_val, keywords):
    celda_clean = celda_val.replace('.', ' ').replace('_', ' ')
    words_in_cell = set(celda_clean.split())
    for k in keywords:
        if k == celda_val or k in words_in_cell:
            return True
    return False

@bp_bodegas.route('/bodegas/upload_excel', methods=['POST'])
@csrf.exempt 
def upload_excel():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    empresa_id = str(session.get('empresa_id'))
    
    archivos = request.files.getlist('file')
    if not archivos or all(f.filename == '' for f in archivos): 
        return jsonify({'error': 'No se recibieron archivos'}), 400

    try:
        cur = mysql.connection.cursor()
        
        cur.execute("SELECT sku, ean, producto, fabricante, factor_conversion, unidad_embalaje FROM productos WHERE id_empresa = %s", (empresa_id,))
        db_products = cur.fetchall()
        
        maestra_productos_ean = {}
        maestra_productos_nombre = {}

        for row in db_products:
            sku_val = normalizar_codigo(row[0])
            ean_val = normalizar_codigo(row[1])
            desc_original = str(row[2]).strip() if row[2] else ""
            desc_limpia = limpiar_texto(desc_original)
            marca_original = str(row[3]).strip() if row[3] and str(row[3]).upper() != 'NAN' else "GENERICO"
            factor_conversion = int(row[4]) if len(row) > 4 and row[4] else 1
            unidad_embalaje = str(row[5]).strip().upper() if len(row) > 5 and row[5] else 'UND'
            
            item_data = {'desc': desc_original, 'marca': marca_original, 'ean': ean_val or sku_val, 'factor_conversion': factor_conversion, 'unidad_embalaje': unidad_embalaje}

            if ean_val: maestra_productos_ean[ean_val] = item_data
            if sku_val: maestra_productos_ean[sku_val] = item_data
            if desc_limpia: maestra_productos_nombre[desc_limpia] = item_data

        cur.execute("SELECT ean_promo, nombre_promo, ean_componente, cajas_componente, fracciones_componente FROM promociones_clientes WHERE id_empresa = %s AND estado = 'ACTIVO'", (empresa_id,))
        db_promos = cur.fetchall()
        cur.close()
        
        diccionario_promos = {}
        maestra_promos_nombre = {} 

        for row in db_promos:
            p_padre = normalizar_codigo(row[0]).upper() 
            p_nombre = str(row[1]).strip() if row[1] else "PROMO"
            p_hijo = normalizar_codigo(row[2]).upper()
            p_cajas = int(row[3]) if row[3] is not None else 0
            p_fracc = int(row[4]) if row[4] is not None else 0
            
            if p_padre not in diccionario_promos: 
                diccionario_promos[p_padre] = {'nombre': p_nombre, 'componentes': []}
                nombre_promo_limpio = limpiar_texto(p_nombre)
                if nombre_promo_limpio:
                    maestra_promos_nombre[nombre_promo_limpio] = p_padre
                    
            diccionario_promos[p_padre]['componentes'].append({'ean': p_hijo, 'cajas': p_cajas, 'fracciones': p_fracc})

        resultados_exito = []
        resultados_error = []
        total_items_insertados = 0

        for file in archivos:
            if file.filename == '': continue
            filename = file.filename
            
            try:
                if file.filename.lower().endswith('.csv'):
                    df_raw = pd.read_csv(file, header=None, sep=None, engine='python')
                else:
                    df_raw = pd.read_excel(file, header=None)
                
                df_raw = df_raw.fillna('')

                # 1. Detectar Meta-Data (Planilla y Zona combinando Regex robusto y búsqueda posicional)
                meta_zona = 'GENERAL'
                meta_orden = filename.split('.')[0].replace('_', ' ').strip()
                
                raw_head = [str(x).strip().upper() for x in df_raw.head(20).values.flatten() if pd.notna(x)]
                text_dump = " ".join(raw_head)
                
                match_zona = re.search(r'(ZONA|RUTA|UBICACION|DESTINO)\s*[:#]?\s*(\w+)', text_dump)
                if match_zona: meta_zona = match_zona.group(2)
                
                meta_fecha = datetime.now().strftime('%Y-%m-%d')
                match_fecha = re.search(r'(\d{2,4}[-/]\d{2}[-/]\d{2,4})', text_dump)
                if match_fecha: meta_fecha = match_fecha.group(1)
                
                pedidos_keywords = ['PEDIDO N°', 'ORDEN NUMERO', 'PEDIDO NUMERO', 'ORDEN N°', 'PEDIDO', 'ORDEN', 'PLANILLA', 'REMISION', 'ENTREGA', 'DOC']
                
                for r in range(min(30, len(df_raw))):
                    for c in range(min(30, len(df_raw.columns))):
                        celda_val = str(df_raw.iloc[r, c]).strip().upper()
                        if not celda_val or celda_val in ['NAN', 'NULL', 'NONE']: continue
                        
                        # Buscar Zona (Respaldo por si el regex falló)
                        if meta_zona == 'GENERAL':
                            if celda_val == 'ZONA' or celda_val == 'RUTA':
                                if c + 1 < len(df_raw.columns) and str(df_raw.iloc[r, c+1]).strip() and str(df_raw.iloc[r, c+1]).strip().upper() != 'NAN':
                                    meta_zona = str(df_raw.iloc[r, c+1]).strip()
                                elif r + 1 < len(df_raw) and str(df_raw.iloc[r+1, c]).strip() and str(df_raw.iloc[r+1, c]).strip().upper() != 'NAN':
                                    meta_zona = str(df_raw.iloc[r+1, c]).strip()
                                
                        # Buscar Pedido (Derecha y luego Abajo)
                        if any(k == celda_val for k in pedidos_keywords) or any(k in celda_val for k in pedidos_keywords):
                            if c + 1 < len(df_raw.columns) and str(df_raw.iloc[r, c+1]).strip() and str(df_raw.iloc[r, c+1]).strip().upper() != 'NAN':
                                meta_orden = str(df_raw.iloc[r, c+1]).strip()
                            elif r + 1 < len(df_raw) and str(df_raw.iloc[r+1, c]).strip() and str(df_raw.iloc[r+1, c]).strip().upper() != 'NAN':
                                meta_orden = str(df_raw.iloc[r+1, c]).strip()

                # 2. Identificar Columnas Estratégicamente (Fuzzy Matching)
                col_ean = col_prod = col_cant = col_und_med = col_cajas = col_unidades = None
                
                keywords_cols = {
                    'CODIGO': ['CODIGO', 'EAN', 'ITEM', 'SKU', 'REF', 'MATERIAL', 'BARCODE'],
                    'DESCRIPCION': ['DESCRIPCION', 'DESCRIPCIÓN', 'PRODUCTO', 'NOMBRE', 'DETALLE', 'TEXTO', 'MERCANCIA', 'ARTICULO', 'ART.', 'ART'],
                    'CANTIDAD': ['CANTIDAD', 'CANT', 'QTY', 'QUANTITY', 'SOLICITADO', 'PZS', 'PIEZAS'],
                    'UNIDAD_MEDIDA': ['UNIDAD', 'UND', 'UM', 'U.M.', 'MEDIDA', 'EMPAQUE', 'PRESENTACION', 'TIPO'],
                    'CAJAS': ['CAJAS', 'CAJA', 'CJ', 'CJS', 'BULTOS', 'BTO', 'PACAS', 'CARTON', 'EMP'],
                    'UNIDADES': ['UNIDADES', 'UND', 'UNDS', 'FRACCIONES', 'FRACC', 'SUELTAS', 'PAQUETES', 'PQT']
                }

                start_row = 0
                header_map = {}
                found_table = False

                for r in range(min(30, len(df_raw))):
                    temp_map = {}
                    for c in range(len(df_raw.columns)):
                        val_raw = str(df_raw.iloc[r, c]).strip()
                        if not val_raw: continue
                        val_clean = limpiar_texto(val_raw)
                        
                        for key, words in keywords_cols.items():
                            if match_keyword(val_clean, words):
                                if key not in temp_map: temp_map[key] = c
                                break
                    
                    if ('DESCRIPCION' in temp_map) and (('CAJAS' in temp_map or 'UNIDADES' in temp_map) or ('CANTIDAD' in temp_map)):
                        start_row = r + 1; header_map = temp_map; found_table = True; break

                # Fallback por inspección estructural si no se detectó por encabezados directos
                if not found_table:
                    for j in range(len(df_raw)):
                        row_fallback = df_raw.iloc[j]
                        row_str = [str(x) for x in row_fallback.values]
                        col_code = None; col_desc = None; col_cant1 = None; col_cant2 = None
                        
                        for col_idx, val in enumerate(row_str):
                            val_clean = val.strip()
                            if val_clean.upper() in ['NAN', '']: continue
                            
                            if col_code is None and re.match(r'^\d{3,20}$', val_clean): col_code = col_idx
                            elif col_desc is None and len(val_clean) > 5 and any(c.isalpha() for c in val_clean): col_desc = col_idx
                            elif col_cant1 is None and re.match(r'^\d+(\.\d+)?$', val_clean) and float(val_clean) < 10000: col_cant1 = col_idx
                            elif col_cant2 is None and re.match(r'^\d+(\.\d+)?$', val_clean) and float(val_clean) < 10000: col_cant2 = col_idx
                        
                        if col_desc is not None and (col_cant1 is not None or col_cant2 is not None):
                            header_map = {'CODIGO': col_code, 'DESCRIPCION': col_desc}
                            if col_cant1 is not None: header_map['CAJAS'] = col_cant1
                            if col_cant2 is not None: header_map['UNIDADES'] = col_cant2
                            start_row = j
                            found_table = True
                            break

                if not found_table: 
                    resultados_error.append(f"❌ {filename}: No se detectaron columnas válidas ni patrones de datos.")
                    continue 

                # 3. Procesamiento y Comparación Fila por Fila
                data_to_insert = []
                fecha_creacion = datetime.now()
                
                cálculo_total_cajas = 0
                cálculo_total_unidades = 0
                cálculo_marcas_presentes = set()
                
                lineas_detectadas = 0
                lineas_ignoradas = 0
                sub_items_generados = 0

                for i in range(start_row, len(df_raw)):
                    row = df_raw.iloc[i]
                    
                    idx_desc = header_map.get('DESCRIPCION')
                    idx_code = header_map.get('CODIGO')
                    
                    # REGLA ESTRICTA DE FILA VACÍA: Si la descripción está vacía, se salta TODA la fila contigua de inmediato
                    raw_desc = str(row[idx_desc]).strip() if idx_desc is not None and pd.notna(row[idx_desc]) else ""
                    if raw_desc.upper() in ['NAN', 'NULL', 'NONE', '']: 
                        continue
                    
                    lineas_detectadas += 1
                    
                    # Devorar dinámicamente cualquier cantidad de puntos suspensivos de relleno contable al final
                    raw_desc = re.sub(r'[\.\s]+$', '', raw_desc).strip()
                    
                    raw_code = str(row[idx_code]).strip() if idx_code is not None and pd.notna(row[idx_code]) else ""
                    if raw_code.upper() in ['NAN', 'NULL', 'NONE']: raw_code = ""
                    raw_code = normalizar_codigo(raw_code)

                    cajas = 0
                    unidades = 0
                    
                    # Jerarquía Estricta: Prioridad Caso 1 (Cantidad/Unidad) sobre Caso 2 (Pivote)
                    if 'CANTIDAD' in header_map and 'UNIDAD_MEDIDA' in header_map:
                        val_cant = row[header_map['CANTIDAD']]
                        if str(val_cant).strip() and es_cadena_numerica(val_cant):
                            cant_num = int(float(val_cant))
                            str_unidad = str(row[header_map['UNIDAD_MEDIDA']]).strip().upper()
                            
                            if str_unidad in ['CAJA', 'CJ', 'CAJAS', 'BTO', 'BULTOS']: cajas = cant_num
                            else: unidades = cant_num
                    else:
                        if 'CAJAS' in header_map:
                            val = row[header_map['CAJAS']]
                            if str(val).strip() and es_cadena_numerica(val): cajas = int(float(val))
                        if 'UNIDADES' in header_map:
                            val = row[header_map['UNIDADES']]
                            if str(val).strip() and es_cadena_numerica(val): unidades = int(float(val))
                            
                    if cajas <= 0 and unidades <= 0: 
                        lineas_ignoradas += 1
                        continue

                    final_ean = raw_code
                    final_desc = raw_desc
                    final_marca = 'GENERICO'
                    auth = False 

                    match_encontrado = False
                    es_promo = False

                    # 1. Comparar por EAN (Prioridad)
                    if final_ean:
                        if final_ean in diccionario_promos:
                            es_promo = True
                            match_encontrado = True
                        elif final_ean in maestra_productos_ean:
                            prod_db = maestra_productos_ean[final_ean]
                            final_desc = prod_db['desc']
                            final_marca = prod_db['marca']
                            match_encontrado = True
                            
                            if prod_db['unidad_embalaje'] in ['CAJA', 'CJ', 'CAJAS'] and unidades > 0 and cajas == 0:
                                cajas = unidades; unidades = 0
                            elif prod_db['unidad_embalaje'] not in ['CAJA', 'CJ', 'CAJAS'] and cajas > 0 and unidades == 0:
                                unidades = cajas; cajas = 0

                    # 2. Comparar por Descripción EXACTA (Sin tolerancias)
                    if not match_encontrado and final_desc:
                        desc_limpia = limpiar_texto(final_desc)
                        
                        if desc_limpia in maestra_promos_nombre:
                            final_ean = maestra_promos_nombre[desc_limpia]
                            es_promo = True
                            match_encontrado = True
                        elif desc_limpia in maestra_productos_nombre:
                            prod_db = maestra_productos_nombre[desc_limpia]
                            final_ean = prod_db['ean']
                            final_desc = prod_db['desc']
                            final_marca = prod_db['marca']
                            match_encontrado = True
                            
                            if prod_db['unidad_embalaje'] in ['CAJA', 'CJ', 'CAJAS'] and unidades > 0 and cajas == 0:
                                cajas = unidades; unidades = 0
                            elif prod_db['unidad_embalaje'] not in ['CAJA', 'CJ', 'CAJAS'] and cajas > 0 and unidades == 0:
                                unidades = cajas; cajas = 0

                    if match_encontrado:
                        auth = True 
                    else:
                        final_marca = 'NO EN BASE DE DATOS'
                        if not final_ean: final_ean = 'SIN_CODIGO'

                    if es_promo:
                        promo_info = diccionario_promos[final_ean]
                        nombre_promo = promo_info['nombre']
                        
                        factor_promo = 1
                        if final_ean in maestra_productos_ean:
                            factor_promo = maestra_productos_ean[final_ean].get('factor_conversion', 1)
                        
                        total_promos_pedidas = (cajas * factor_promo) + unidades 
                        
                        for comp in promo_info['componentes']:
                            hijo_code = comp['ean']
                            hijo_cajas_total = total_promos_pedidas * comp['cajas'] 
                            hijo_unid_total = total_promos_pedidas * comp['fracciones'] 
                            hijo_desc = f"ITEM SIN NOMBRE ({hijo_code})"
                            hijo_marca = 'GENERICO'
                            
                            if hijo_code in maestra_productos_ean:
                                hijo_desc = maestra_productos_ean[hijo_code]['desc']
                                hijo_marca = maestra_productos_ean[hijo_code]['marca']
                            
                            hijo_desc_visual = f"{hijo_desc} (Kit: {nombre_promo})"
                            
                            if hijo_cajas_total > 0 or hijo_unid_total > 0:
                                data_to_insert.append((empresa_id, meta_orden, meta_zona, hijo_code, raw_code, hijo_desc_visual, hijo_marca, hijo_cajas_total, 0, hijo_unid_total, 0, 'PENDIENTE', fecha_creacion, meta_fecha, auth))
                                cálculo_total_cajas += hijo_cajas_total
                                cálculo_total_unidades += hijo_unid_total
                                cálculo_marcas_presentes.add(hijo_marca)
                                sub_items_generados += 1
                    else:
                        # Detectar promociones huérfanas
                        palabras_clave_promo = ['OFE ', 'OFERTA', 'PROMO', 'KIT ', 'COMBO', 'PAGUE ']
                        desc_upper = str(final_desc).upper() if final_desc else ""
                        es_huerfana = any(k in desc_upper for k in palabras_clave_promo)
                        
                        estado_insert = 'PROMO_HUERFANA' if es_huerfana else 'PENDIENTE'
                        auth_insert = False if es_huerfana else auth

                        data_to_insert.append((empresa_id, meta_orden, meta_zona, final_ean, raw_code, final_desc, final_marca, cajas, 0, unidades, 0, estado_insert, fecha_creacion, meta_fecha, auth_insert))
                        cálculo_total_cajas += cajas
                        cálculo_total_unidades += unidades
                        cálculo_marcas_presentes.add(final_marca)

                if data_to_insert:
                    cur = mysql.connection.cursor()
                    query = """INSERT INTO picking_importacion_raw 
                    (id_empresa, numero_orden_origen, zona, codigo_producto, ean_leido, descripcion_producto, marca, cajas_calculadas, cajas_alistadas, unidades_calculadas, unidades_alistadas, estado_actividad, fecha_creacion_orden, fecha_entrega_orden, autorizacion_alistamiento) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    cur.executemany(query, data_to_insert)
                    mysql.connection.commit()
                    cur.close()
                    
                    resultados_exito.append(
                        f"✅ {meta_orden}\n"
                        f"📊 Reporte de Carga Transparente:\n"
                        f"• Líneas detectadas en Excel: {lineas_detectadas}\n"
                        f"• Líneas ignoradas (Cantidades en cero): {lineas_ignoradas}\n"
                        f"• Kits/Promociones desarmadas: +{sub_items_generados} sub-ítems generados\n"
                        f"• Total final de líneas a preparar en Bodega: {len(data_to_insert)}\n"
                        f"• Auditoría Final: {cálculo_total_cajas} Cajas y {cálculo_total_unidades} Unidades."
                    )
                    total_items_insertados += len(data_to_insert)
                else:
                    resultados_error.append(f"❌ {filename}: Sin items válidos para insertar.")

            except Exception as e:
                resultados_error.append(f"❌ {filename}: Error de lectura ({str(e)})")

        mensaje_alerta = ""
        if resultados_exito: mensaje_alerta += "\n\n".join(resultados_exito) + "\n\n"
        if resultados_error: mensaje_alerta += "NO SE PUDIERON SUBIR:\n" + "\n".join(resultados_error)

        return jsonify({'message': mensaje_alerta, 'recargar': len(resultados_exito) > 0})

    except Exception as e:
        return jsonify({'error': f'Error crítico procesando carga: {str(e)}'}), 500
    
@bp_bodegas.route('/api/bodegas/reportes/importacion')
def reporte_importacion():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                numero_orden_origen as orden, 
                MAX(zona) as zona,
                COUNT(*) as total_items,
                SUM(CASE WHEN estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'TERMINADO') THEN 1 ELSE 0 END) as items_procesados,
                MAX(fecha_carga) as fecha_carga
            FROM picking_importacion_raw
            WHERE id_empresa = %s
            GROUP BY numero_orden_origen
            ORDER BY MAX(fecha_carga) DESC LIMIT 100
        """, (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        
        for row in data:
            if isinstance(row['fecha_carga'], datetime):
                row['fecha_carga'] = row['fecha_carga'].strftime('%Y-%m-%d %H:%M:%S')
                
        return jsonify(data)
    except Exception as e:
        print(f"Error reporte importacion: {e}")
        return jsonify([])