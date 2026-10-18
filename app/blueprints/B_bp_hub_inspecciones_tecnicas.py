# MÓDULO: OPERACIONES | SUBMÓDULO: HUB_INSPECCIONES | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom

# Se define el Blueprint siguiendo la nomenclatura estricta Plug & Play
B_bp_hub_inspecciones_tecnicas = Blueprint('B_bp_hub_inspecciones_tecnicas', __name__)

def _obtener_modulos_inspecciones():
    """
    Arquitectura Data-Driven: Define los submódulos exclusivos pertenecientes a la 
    Unidad de Negocio OPERACIONES (Inspecciones Técnicas a Clientes).
    Reemplaza la lógica legacy de 'Reportes de visitas' organizándola en tarjetas.
    """
    return [
        {
            "nombre": "Programación de Visitas",
            "ruta": "#", # Ruta a reemplazar cuando el controlador específico sea refactorizado
            "icono": "📅",
            "color": "primary",
            "descripcion": "Agendamiento y asignación de técnicos para visitas en campo."
        },
        {
            "nombre": "Reportes de Campo",
            "ruta": "#", 
            "icono": "📋",
            "color": "warning",
            "descripcion": "Revisión de actas, checklists de inspección y registro fotográfico."
        },
        {
            "nombre": "Aprobación y Certificación",
            "ruta": "#", 
            "icono": "✅",
            "color": "success",
            "descripcion": "Auditoría final y emisión de certificados de inspección para clientes."
        }
    ]

@B_bp_hub_inspecciones_tecnicas.route('/hub_inspecciones_tecnicas_clientes.html')
@login_required_custom
def hub_inspecciones():
    """
    Controlador centralizado para la renderización dinámica del Hub Inspecciones Técnicas.
    Aplica protección de rutas validando el rol exacto asignado a esta Unidad de Negocio.
    """
    perfil = str(session.get('perfil', '')).strip().lower()
    empresa_id = str(session.get('empresa_id', '')).strip()

    # Validación estricta de seguridad (RBAC) y aislamiento Tenant corporativo
    perfiles_permitidos = [
        'webmaster_inspecciones', 
        'gerente_bqa', 
        'webmaster', 
        'webmaster_admin'
    ]
    
    if perfil not in perfiles_permitidos or empresa_id != '901811727':
        flash("Acceso denegado: No posees los privilegios requeridos para gestionar la Unidad de Operaciones (Inspecciones).", "danger")
        return redirect(url_for('index'))

    # Se inyecta la matriz Data-Driven a la plantilla
    modulos = _obtener_modulos_inspecciones()
    
    return render_template(
        'B_hub_inspecciones_tecnicas_clientes.html',
        nombre=session.get('nombre', 'Webmaster Inspecciones'),
        empresa=session.get('empresa', 'Baquía-ems SAS'),
        nit=empresa_id,
        modulos=modulos
    )