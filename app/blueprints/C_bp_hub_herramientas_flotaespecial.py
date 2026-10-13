# MÓDULO: Transporte Especial | SUBMÓDULO: Hub Herramientas Flota Especial | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/C_bp_hub_herramientas_flotaespecial.py

from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom

# Definición del Blueprint para el Sub-submódulo Concentrador
bp_hub_herramientas_flota = Blueprint(
    'hub_herramientas_flota', 
    __name__, 
    url_prefix='/gestor_flotaespecial'
)

@bp_hub_herramientas_flota.route('/hub_herramientas', methods=['GET'])
@login_required_custom
def hub_herramientas():
    """
    Sub-ruta concentradora (Tipo 1) que aloja las herramientas operativas
    de la flota física (Gestión de Vehículos, Mantenimiento, Combustible, PESV).
    """
    # Se obtienen las variables de sesión para la interfaz y el control Multi-Tenant
    usuario = session.get('nombre', 'Usuario')
    empresa = session.get('empresa', 'Empresa')
    
    # Renderizado de la pareja frontend correspondiente
    return render_template(
        'C_hub_herramientas_flotaespecial.html',
        usuario=usuario,
        empresa=empresa
    )