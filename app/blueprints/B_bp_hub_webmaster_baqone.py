# MÓDULO: BQA_ONE | SUBMÓDULO: HUB_WEBMASTER_BAQONE | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom

# Se define el Blueprint siguiendo la nomenclatura estricta Plug & Play
B_bp_hub_webmaster_baqone = Blueprint('B_bp_hub_webmaster_baqone', __name__)

def _obtener_modulos_bqa_one():
    """
    Arquitectura Data-Driven: Define los submódulos exclusivos pertenecientes a la 
    Unidad de Negocio BQA-ONE (División Software). Aplica los nuevos nombres front-end.
    """
    return [
        {
            "nombre": "GESTIÓN CLIENTES BQA-ONE",
            "ruta": "bp_901811727_main.panel_admin", 
            "icono": "🏢",
            "color": "primary",
            "descripcion": "Administración central, CRUD de empresas, usuarios y asignación de perfiles."
        },
        {
            "nombre": "MONITOREO BQA-ONE",
            "ruta": "bp_901811727_main.panel_audit", 
            "icono": "👁️",
            "color": "info",
            "descripcion": "Auditoría en tiempo real, lectura de Audit.log y Health Check del servidor."
        }
    ]

@B_bp_hub_webmaster_baqone.route('/hub_webmaster_baqone.html')
@login_required_custom
def hub_webmaster():
    """
    Controlador centralizado para la renderización dinámica del Hub BQA-ONE.
    Aplica protección de rutas validando el rol exacto asignado a esta Unidad de Negocio,
    permitiendo también el acceso de perfiles jerárquicamente superiores (Gerente).
    """
    perfil = str(session.get('perfil', '')).strip().lower()
    empresa_id = str(session.get('empresa_id', '')).strip()

    # Validación estricta de seguridad (RBAC) y aislamiento Tenant corporativo
    perfiles_permitidos = ['webmaster_baqone', 'gerente_bqa', 'webmaster', 'webmaster_admin']
    
    if perfil not in perfiles_permitidos or empresa_id != '901811727':
        flash("Acceso denegado: No posees los privilegios requeridos para gestionar la Unidad BQA-ONE.", "danger")
        return redirect(url_for('index'))

    # Se inyecta la matriz Data-Driven a la plantilla
    modulos = _obtener_modulos_bqa_one()
    
    return render_template(
        'B_hub_webmaster_baqone.html',
        nombre=session.get('nombre', 'Webmaster BQA-ONE'),
        empresa=session.get('empresa', 'Baquía-ems SAS'),
        nit=empresa_id,
        modulos=modulos
    )