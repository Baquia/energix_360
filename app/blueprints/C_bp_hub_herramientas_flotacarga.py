# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: HUB_HERRAMIENTAS | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/C_bp_hub_herramientas_flotacarga.py
import json
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps

bp_hub_herramientas_flota = Blueprint('hub_herramientas_flotacarga', __name__, url_prefix='/gestor_flota/hub_herramientas')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS (CORREGIDO - ANTI BUCLE)
# ==============================================================================
def herramientas_flota_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'gestor_flotacarga', 
            'controlador_transportecarga', 
            'controlador_flotacarga', 
            'controlador_logistica', 
            'webmaster'
        ]
        
        # 1. Validación de Perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Gestor/Controlador de Flota para ingresar a este módulo.', 'danger')
            return redirect('/gestor_flota/dashboard')
            
        # 2. Validación Plug & Play (Inquilino - Anti Bucle)
        if 'webmaster' not in tipo_empresa:
            autorizados = session.get('submodulos_activos', [])
            if not autorizados:
                autorizados = session.get('modulos_activos', [])
                
            if not any(m in autorizados for m in ['controlador_flota', 'flota', 'carga']):
                flash('Acceso denegado: Tu empresa no tiene contratado/activo el submódulo de Controlador de Flota.', 'danger')
                return redirect('/gestor_flota/dashboard')
                
        return f(*args, **kwargs)
    return decorated_function

@bp_hub_herramientas_flota.route('/')
@login_required_custom
@herramientas_flota_required
def index():
    return render_template(
        'C_hub_herramientas_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        modulos_activos=session.get('modulos_activos', []),
        submodulos_activos=session.get('submodulos_activos', []),
        perfil=session.get('perfil')
    )