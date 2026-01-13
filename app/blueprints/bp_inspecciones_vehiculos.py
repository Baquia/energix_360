from flask import render_template
from app.utils import login_required_custom

# YA NO IMPORTAMOS bp_transporte_especial AQUÍ

def init_routes(bp):
    """
    Inyecta las rutas de inspecciones al blueprint que se le pase.
    """
    
    @bp.route('/app/vehiculo/<string:placa>/inspeccion')
    @login_required_custom
    def inspeccion_diaria(placa):
        return f"<h1>Inspección Pre-operacional para {placa}</h1><p>En construcción...</p>"