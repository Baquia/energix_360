from flask import Blueprint, render_template

# Esta es la línea exacta que Flask está buscando y no encuentra:
gestionavicola_bp = Blueprint('gestionavicola_bp', __name__)

# ==============================================================================
# RUTAS OFFLINE (SERVICE WORKER / PWA)
# Conservadas por retrocompatibilidad de caché en dispositivos móviles
# ==============================================================================
@gestionavicola_bp.route("/gestion_avicola_offline.html")
def panel_avicola_offline():
    # Renderiza la vista estática para cuando no hay internet
    return render_template("gestion_avicola_offline.html")

@gestionavicola_bp.route("/glp_offline.html")
def glp_offline():
    # Renderiza el validador offline de tanques
    return render_template("glp_offline.html")