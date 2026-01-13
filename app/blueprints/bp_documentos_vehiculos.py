# ARCHIVO: bp_documentos_vehiculos.py (REUTILIZABLE)
from flask import render_template, request, redirect, flash, session, url_for
from app.utils import login_required_custom
from app import mysql
from datetime import datetime
import os
from werkzeug.utils import secure_filename

# YA NO IMPORTAMOS AL PADRE AQUÍ. ESTE ARCHIVO ES INDEPENDIENTE.

def init_routes(bp):
    """
    Esta función recibe CUALQUIER blueprint (sea de transporte especial, 
    cautivo, o pollos gar) y le inyecta las rutas de documentos.
    """

    # --- RUTA 1: VER DOCUMENTOS ---
    @bp.route('/app/vehiculo/<string:placa>/documentos')
    @login_required_custom
    def ver_documentos_vehiculo(placa):
        # NOTA: Como la ruta es relativa al blueprint, si el blueprint 
        # tiene url_prefix='/te', la ruta final será '/te/app/...'
        
        nit = str(session.get('empresa_id'))
        
        # AQUÍ VA TU LÓGICA DE CONSULTA DE DOCUMENTOS (IGUAL QUE ANTES)
        # ... (Tu código de consulta SQL, FUEC, etc.) ...
        
        # EJEMPLO SIMPLIFICADO:
        return render_template('documentos_vehiculos.html', 
                               placa=placa, 
                               nit=nit,
                               documentos_portados=[], # Tu lista real
                               fuec=None)

    # --- RUTA 2: SUBIR DOCUMENTO ---
    @bp.route('/admin/subir_documento', methods=['POST'])
    @login_required_custom
    def subir_documento():
        # Lógica de subida...
        flash("Documento procesado (Módulo Genérico)", "success")
        return redirect(request.referrer)
    
    # ¡Importante! No retornamos nada, solo registramos las rutas en 'bp'