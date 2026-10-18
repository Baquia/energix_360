# MÓDULO: WEBMASTER_BQAONE | SUBMÓDULO: MAIN_ROUTER | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app import mysql
from app.forms import RegistroUsuarioForm
from app.utils import login_required_custom

bp_main = Blueprint('bp_901811727_main', __name__)

# ==============================================================================
# FUNCIÓN UTILITARIA: EXTRACCIÓN DE DATOS BASE Y CONTROL DE ACCESOS
# ==============================================================================
def _obtener_datos_base():
    """Extrae la lista de empresas, tipos y clientes para inyectarlos en Jinja2"""
    cur = mysql.connection.cursor()
    
    # 1. Obtener empresas y sus módulos activos
    cur.execute("""
        SELECT e.nit, e.nombre_comercial, e.tipo_empresa, 
               GROUP_CONCAT(m.modulo) as modulos
        FROM empresas e
        LEFT JOIN modulos_empresas_autorizadas m ON e.nit = m.id_empresa AND m.estatus = 'activo'
        GROUP BY e.nit, e.nombre_comercial, e.tipo_empresa
    """)
    data_empresas = cur.fetchall()
    
    mapa_normalizacion = {
        'glp': 'gas', 
        'supervisorgas': 'gas', 
        'flotacarga': 'flota', 
        'combustible_flota': 'flota', 
        'gestorflota': 'flota', 
        'preoperacional': 'flota', 
        'gestion_carga': 'carga', 
        'gestionavicola_bp': 'carga', 
        'gestion_mermas': 'mermas',
        'bodegas': 'bodegas',
        'ventas': 'ventas',
        'administracion': 'administracion'
    }

    empresas = []
    if data_empresas:
        if isinstance(data_empresas[0], dict):
            for row in data_empresas:
                mods_raw = row['modulos'].split(',') if row['modulos'] else []
                row['modulos_activos'] = list(set([mapa_normalizacion.get(m, m) for m in mods_raw]))
                empresas.append(row)
        else:
            for row in data_empresas:
                mods_raw = row[3].split(',') if (len(row) > 3 and row[3]) else []
                mods_v2 = list(set([mapa_normalizacion.get(m, m) for m in mods_raw]))
                empresas.append({
                    'nit': row[0], 
                    'nombre_comercial': row[1], 
                    'tipo_empresa': row[2] if row[2] else 'general',
                    'modulos_activos': mods_v2
                })

    # 2. Obtener Tipos de Empresa
    cur.execute("SELECT tipo FROM tipos_empresa")
    data_tipos = cur.fetchall()
    tipos_empresa = [r['tipo'] if isinstance(r, dict) else r[0] for r in data_tipos]
    
    # 3. Obtener Clientes (Lista simple para el menú lateral)
    cur.execute("SELECT nit, nombre_comercial FROM empresas")
    data_clientes = cur.fetchall()
    clientes = []
    for c in data_clientes:
        if isinstance(c, dict):
            clientes.append(c)
        else:
            clientes.append({'nit': c[0], 'nombre_comercial': c[1]})
            
    cur.close()
    
    return empresas, tipos_empresa, clientes

def _validar_acceso_submodulo(roles_permitidos):
    """Valida si el perfil en sesión tiene acceso al submódulo o si es Super-Webmaster / Gerente Corporativo"""
    perfil = str(session.get('perfil', '')).strip().lower()
    empresa_id = str(session.get('empresa_id', '')).strip()
    
    # Perfiles con privilegios universales corporativos en el tenant Baquía-ems SAS
    roles_gerenciales = [
        'gerente_bqa', 
        'webmaster', 
        'webmaster_admin', 
        'admin_general'
    ]
    if perfil in roles_gerenciales or (empresa_id == '901811727' and 'webmaster' in perfil):
        return True
        
    return perfil in roles_permitidos

# ==============================================================================
# ENRUTADORES DE SUBMÓDULOS (VISTAS)
# ==============================================================================

@bp_main.route('/901811727.html')
@login_required_custom
def panel_webmaster():
    """
    Ruta de compatibilidad legacy: redirige dinámicamente al nuevo Super-Hub Gerencial
    evitando el renderizado del monolito descontinuado.
    """
    return redirect('/hub_gerencia_baquia.html')

@bp_main.route('/901811727_admin.html')
@login_required_custom
def panel_admin():
    """Renderiza el submódulo de Administración (CRUD Multiempresa)"""
    roles_validos = ['webmaster_admin', 'webmaster_baqone']
    if not _validar_acceso_submodulo(roles_validos):
        flash("No tienes permisos suficientes para acceder al módulo de Administración.", "warning")
        return redirect('/hub_gerencia_baquia.html')

    form = RegistroUsuarioForm()
    empresas, tipos_empresa, clientes = _obtener_datos_base()
    
    return render_template(
        '901811727_admin.html', 
        nombre=session.get('nombre', 'Administrador'), 
        empresa=session.get('empresa', 'Baquía-ems SAS'), 
        nit=session.get('empresa_id', '901811727'),
        form=form, 
        empresas=empresas, 
        tipos_empresa=tipos_empresa,
        clientes=clientes
    )

@bp_main.route('/901811727_energia.html')
@login_required_custom
def panel_energia():
    """Renderiza el submódulo de Gestión Energética (GLP y Auditorías)"""
    roles_validos = ['webmaster_energia', 'webmaster_gestionenergetica', 'webmaster_operaciones']
    if not _validar_acceso_submodulo(roles_validos):
        flash("No tienes permisos suficientes para acceder al módulo de Gestión Energética.", "warning")
        return redirect('/hub_gerencia_baquia.html')

    empresas, _, clientes = _obtener_datos_base()
    
    return render_template(
        '901811727_energia.html', 
        nombre=session.get('nombre', 'Gestor Energético'), 
        empresa=session.get('empresa', 'Baquía-ems SAS'), 
        nit=session.get('empresa_id', '901811727'),
        empresas=empresas,
        clientes=clientes
    )

@bp_main.route('/901811727_audit.html')
@login_required_custom
def panel_audit():
    """Renderiza el submódulo de Auditoría de Sistema (Health Check y Audit.log)"""
    roles_validos = ['webmaster_audit', 'webmaster_baqone', 'webmaster_soporte']
    if not _validar_acceso_submodulo(roles_validos):
        flash("No tienes permisos suficientes para acceder al módulo de Auditoría.", "warning")
        return redirect('/hub_gerencia_baquia.html')

    empresas, _, _ = _obtener_datos_base()
    
    return render_template(
        '901811727_audit.html', 
        nombre=session.get('nombre', 'Auditor de Sistema'), 
        empresa=session.get('empresa', 'Baquía-ems SAS'), 
        nit=session.get('empresa_id', '901811727'),
        empresas=empresas
    )