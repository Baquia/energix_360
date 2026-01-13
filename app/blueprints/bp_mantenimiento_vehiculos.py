from flask import render_template
from app.utils import login_required_custom

# YA NO IMPORTAMOS bp_transporte_especial AQUÍ

def init_routes(bp):
    """
    Inyecta las rutas de mantenimiento al blueprint que se le pase.
    """
    
    # Nota: Quitamos el '/te' del inicio porque el padre debería poner el prefijo,
    # o si no tiene prefijo, la ruta será /app/vehiculo/...
    @bp.route('/app/vehiculo/<string:placa>/mantenimiento')
    @login_required_custom
    def mantenimiento_home(placa):
        return f"<h1>Módulo de Mantenimiento para {placa}</h1><p>En construcción...</p>"