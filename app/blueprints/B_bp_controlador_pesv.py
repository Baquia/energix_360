# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: PESV | CONDICIÓN: OPCIONAL | TIPO: 2
# app/blueprints/B_bp_controlador_pesv.py
from flask import Blueprint, render_template, session, redirect, url_for, flash, jsonify
from app import mysql
from app.utils import login_required_custom, submodulo_required
from functools import wraps
import MySQLdb.cursors

bp_controlador_pesv = Blueprint('controlador_pesv', __name__, url_prefix='/gestor_flota/pesv_bp')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS MULTI-MÓDULO
# ==============================================================================
def pesv_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'controlador_pesv', 
            'gestor_flotacarga', 
            'controlador_transportecarga', 
            'controlador_flotacarga', 
            'controlador_flotaespecial',
            'controlador_logistica', 
            'webmaster'
        ]
        
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Flota para ingresar al módulo PESV.', 'danger')
            return redirect(url_for('index'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# RUTAS DEL SUBMÓDULO
# ==============================================================================
@bp_controlador_pesv.route('/', methods=['GET'])
@login_required_custom
@pesv_required
@submodulo_required('pesv')
def index_pesv():
    """
    Controlador raíz del Plan Estratégico de Seguridad Vial (PESV).
    Renderiza la vista principal para analítica y gestión segmentada según el tipo de flota.
    """
    empresa_id = session.get('empresa_id')
    tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
    
    # Estructura de los KPIs
    kpis = {
        'conductores_activos': 0,
        'alertas_documentos_cond': 0,
        'horas_conduccion_hoy': 0,
        'vehiculos_bloqueados': 0,
        'novedades_ruta': 0
    }
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # ======================================================================
        # 1. PILARES ESPECÍFICOS SEGÚN EL TIPO DE EMPRESA (CARGA VS ESPECIAL)
        # ======================================================================
        if tipo_empresa == 'transporte_carga':
            # --- Comportamiento Humano: Documentos y Activos (Carga) ---
            cur.execute("""
                SELECT COUNT(*) as total FROM conductores_carga 
                WHERE id_empresa = %s AND estatus != 'Inactivo'
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['conductores_activos'] = resultado['total'] if resultado else 0
            
            cur.execute("""
                SELECT COUNT(*) as alertas FROM conductores_carga 
                WHERE id_empresa = %s 
                AND (vencimiento_licencia_conduccion < CURDATE() OR vencimiento_seguridad_social < CURDATE())
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['alertas_documentos_cond'] = resultado['alertas'] if resultado else 0
            
            # --- Comportamiento Humano: Fatiga y Jornadas (Carga) ---
            cur.execute("""
                SELECT SUM(TIMESTAMPDIFF(HOUR, fecha_login, IFNULL(fecha_logout_manual, NOW()))) as horas_hoy
                FROM historial_sesiones_flota
                WHERE id_empresa = %s AND DATE(fecha_login) = CURDATE()
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['horas_conduccion_hoy'] = int(resultado['horas_hoy'] or 0) if resultado else 0
            
            # --- Infraestructura Segura: Novedades Viales (Carga) ---
            cur.execute("""
                SELECT COUNT(*) as novedades FROM novedades_viales_flotacarga 
                WHERE id_empresa = %s AND DATE(fecha_hora) = CURDATE()
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['novedades_ruta'] = resultado['novedades'] if resultado else 0

        elif tipo_empresa == 'transporte_especial':
            # --- Comportamiento Humano: Documentos y Activos (Especial) ---
            cur.execute("""
                SELECT COUNT(*) as total FROM conductores_flotaespecial 
                WHERE id_empresa = %s AND estatus != 'Inactivo'
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['conductores_activos'] = resultado['total'] if resultado else 0
            
            cur.execute("""
                SELECT COUNT(*) as alertas FROM conductores_flotaespecial 
                WHERE id_empresa = %s 
                AND (vencimiento_licencia_conduccion < CURDATE() OR vencimiento_seguridad_social < CURDATE())
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['alertas_documentos_cond'] = resultado['alertas'] if resultado else 0
            
            # --- Comportamiento Humano: Fatiga y Jornadas (Especial) ---
            cur.execute("""
                SELECT SUM(TIMESTAMPDIFF(HOUR, fecha_login, IFNULL(fecha_logout_manual, NOW()))) as horas_hoy
                FROM historial_sesiones_flotaespecial
                WHERE id_empresa = %s AND DATE(fecha_login) = CURDATE()
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['horas_conduccion_hoy'] = int(resultado['horas_hoy'] or 0) if resultado else 0
            
            # --- Infraestructura Segura: Novedades Viales (Especial) ---
            cur.execute("""
                SELECT COUNT(*) as novedades FROM control_viajes_flota_especial 
                WHERE id_empresa = %s 
                AND estado_novedad IS NOT NULL AND estado_novedad != '' 
                AND DATE(fecha_novedad) = CURDATE()
            """, (empresa_id,))
            resultado = cur.fetchone()
            kpis['novedades_ruta'] = resultado['novedades'] if resultado else 0

        # ======================================================================
        # 2. PILAR VEHÍCULOS SEGUROS (COMPARTIDO PERO AISLADO POR EMPRESA)
        # ======================================================================
        cur.execute("""
            SELECT COUNT(DISTINCT v.id) as vehiculos_bloqueados
            FROM vehiculos v
            LEFT JOIN inspeccion_preoperacional i 
              ON v.placa = i.placa_vehiculo 
              AND i.fecha_inspeccion = CURDATE() 
              AND i.id_empresa = v.id_empresa
            WHERE v.id_empresa = %s
            AND (
                v.vencimiento_soat < CURDATE() 
                OR v.vencimiento_tecnomecanica < CURDATE()
                OR i.vehiculo_aprobado = 0
            )
        """, (empresa_id,))
        resultado = cur.fetchone()
        kpis['vehiculos_bloqueados'] = resultado['vehiculos_bloqueados'] if resultado else 0

        cur.close()
    except Exception as e:
        print(f"Error consultando analítica PESV: {e}")
    
    return render_template(
        'B_controlador_pesv.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='dashboard',
        kpis=kpis
    )