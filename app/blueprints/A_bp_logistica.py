from flask import Blueprint, render_template, session, redirect, url_for, flash
from app import mysql

# Definimos el Blueprint
logistica_bp = Blueprint('logistica_bp', __name__)

# ==============================================================================
# CONFIGURACIÓN DE ACCESOS SIMPLIFICADA
# ==============================================================================
ACCESO_MODULOS = {
    "aws": {
        # ROL 1: Operario (Hace picking O bodega) -> VA A LA CENTRAL DE BODEGAS
        "operador_logistica": "B_bodegas.html", 
        
        # ROL 2: Auxiliar (Si existe) -> VA A LA CENTRAL DE BODEGAS
        "auxiliar_bodega":    "B_bodegas.html",              
        
        # ROL 3: Jefe/Controlador -> VA AL DASHBOARD DE GESTIÓN
        "controlador_logistica": "B_control_logistica.html",
    },
    "flota_carga": {
        "operador_transportecarga": "B_dashboard_tc.html",
    }
}

@logistica_bp.route('/control_logistica.html')
def home():
    if 'usuario_id' not in session:
        return redirect('/')
    
    nit = str(session.get('empresa_id'))
    nombre = session.get('nombre')
    empresa = session.get('empresa')

    return render_template('A_control_logistica.html', 
                           nit=nit, 
                           nombre=nombre, 
                           empresa=empresa)

# ==============================================================================
# ROUTER UNIVERSAL
# ==============================================================================
@logistica_bp.route('/logistica/router/<modulo>')
def router_universal(modulo):
    usuario_id = session.get('usuario_id')
    if not usuario_id:
        return redirect('/')

    # 1. CONSULTA DE PERFIL EN VIVO
    perfil_real = ""
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT perfil FROM usuarios WHERE id = %s", (usuario_id,))
        row = cur.fetchone()
        cur.close()
        
        if row:
            if isinstance(row, dict):
                perfil_real = str(row.get('perfil', '')).strip()
            else:
                perfil_real = str(row[0]).strip()
        session['perfil'] = perfil_real
        
    except Exception as e:
        print(f"Error consultando perfil: {e}")
        perfil_real = (session.get('perfil') or '').strip()

    # 2. VALIDAR ACCESO
    reglas_modulo = ACCESO_MODULOS.get(modulo)
    if not reglas_modulo:
        flash(f"Error: El módulo '{modulo}' no existe.", "danger")
        return redirect(url_for('logistica_bp.home'))

    # 3. BUSCAR DESTINO
    archivo_destino = reglas_modulo.get(perfil_real)
    
    if not archivo_destino:
        perfil_lower = perfil_real.lower()
        for p_key, archivo in reglas_modulo.items():
            if p_key.lower() == perfil_lower:
                archivo_destino = archivo
                break

    if archivo_destino:
        # ======================================================================
        # REDIRECCIONES A MÓDULOS (Ahora todo pasa por Bodegas o Flota)
        # ======================================================================
        
        # Caso 1: Flota
        if archivo_destino == "B_dashboard_tc.html":
            return redirect(url_for('flotacarga.home'))
            
        # Caso 2: Operación Bodega (Picking o Inventario)
        # Aquí llegan tanto el 'operador_logistica' como el 'auxiliar'
        elif archivo_destino == "B_bodegas.html":
            return redirect(url_for('bodegas.home'))
            
        # Caso 3: Control y Gestión (Jefe)
        elif archivo_destino == "B_control_logistica.html":
             return redirect(url_for('bodegas.dashboard_control')) 

        # Fallback
        return render_template(archivo_destino)
        
    else:
        flash(f"ACCESO DENEGADO: Tu perfil '{perfil_real}' no tiene permiso.", "danger")
        return redirect(url_for('logistica_bp.home'))

# NOTA: Eliminé la ruta '/alistamiento' ya que la lógica se moverá a B_bp_bodegas.py