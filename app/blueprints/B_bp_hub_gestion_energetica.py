# MÓDULO: GESTION_ENERGETICA | SUBMÓDULO: HUB_GESTION_ENERGETICA | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom

# Se define el Blueprint siguiendo la nomenclatura estricta Plug & Play
B_bp_hub_gestion_energetica = Blueprint('B_bp_hub_gestion_energetica', __name__)

def _obtener_modulos_energia():
    """
    Arquitectura Data-Driven: Define los submódulos exclusivos pertenecientes a la 
    Unidad de Negocio ENERGÉTICA. Desacopla las opciones de la vista monolítica legacy
    convirtiéndolas en tarjetas dinámicas de acceso directo.
    """
    return [
        {
            "nombre": "Validación de Tanqueos",
            "ruta": "bp_901811727_main.panel_energia", 
            "icono": "✅",
            "color": "success",
            "descripcion": "Auditoría de ingresos de GLP en campo y verificación de discrepancias con imágenes testigo."
        },
        {
            "nombre": "Proyección y Pedidos",
            "ruta": "bp_901811727_main.panel_energia", 
            "icono": "📈",
            "color": "primary",
            "descripcion": "Análisis predictivo de consumo, tasa de descenso y aprobación de órdenes de suministro."
        },
        {
            "nombre": "Informes y KPIs Energéticos",
            "ruta": "bp_901811727_main.panel_energia", 
            "icono": "📊",
            "color": "warning",
            "descripcion": "Métricas de rendimiento, eficiencia (kg/ave), saldos al cierre y benchmarking de lotes."
        },
        {
            "nombre": "Gestión de QRs y Tanques",
            "ruta": "bp_901811727_main.panel_energia", 
            "icono": "🛢️",
            "color": "info",
            "descripcion": "Administración de configuración de capacidades y generación de códigos QR por sede."
        }
    ]

@B_bp_hub_gestion_energetica.route('/hub_gestion_energetica.html')
@login_required_custom
def hub_gestion_energetica():
    """
    Controlador centralizado para la renderización dinámica del Hub Gestión Energética.
    Aplica protección de rutas validando el rol exacto asignado a esta Unidad de Negocio.
    """
    perfil = str(session.get('perfil', '')).strip().lower()
    empresa_id = str(session.get('empresa_id', '')).strip()

    # Validación estricta de seguridad (RBAC) y aislamiento Tenant corporativo
    perfiles_permitidos = [
        'webmaster_gestionenergetica', 
        'webmaster_energia', 
        'gerente_bqa', 
        'webmaster', 
        'webmaster_admin'
    ]
    
    if perfil not in perfiles_permitidos or empresa_id != '901811727':
        flash("Acceso denegado: No posees los privilegios requeridos para gestionar la Unidad de Gestión Energética.", "danger")
        return redirect(url_for('index'))

    # Se inyecta la matriz Data-Driven a la plantilla
    modulos = _obtener_modulos_energia()
    
    return render_template(
        'B_hub_gestion_energetica.html',
        nombre=session.get('nombre', 'Webmaster Energía'),
        empresa=session.get('empresa', 'Baquía-ems SAS'),
        nit=empresa_id,
        modulos=modulos
    )