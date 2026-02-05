from flask import Blueprint, render_template, session, redirect, url_for, flash
from app import mysql

logistica_bp = Blueprint('logistica_bp', __name__)

# ==============================================================================
# MAPA DE NAVEGACIÓN (Reglas del Botón AWS)
# ==============================================================================
ACCESO_MODULOS = {
    "aws": {
        # Si es Operario -> Mándalo a la Puerta 3 (Función: bodega_operativa)
        "operador_logistica": "C_bodegas.html", 
        
        # Si es Jefe -> Mándalo a la Puerta 2 (Función del archivo B)
        "controlador_logistica": "B_control_logistica.html",
    },
    "flota_carga": {
        "operador_transportecarga": "B_dashboard_tc.html",
        "controlador_transportecarga": "B_dashboard_tc.html"
    }
}

# --------------------------------------------------------------------------
# RUTA BASE A: EL MENÚ REPARTIDOR (SOLUCIÓN AL SALTO)
# --------------------------------------------------------------------------
@logistica_bp.route('/control_logistica.html')
def home():
    if 'usuario_id' not in session:
        return redirect('/')
    
    # CORRECCIÓN: Renderizamos la plantilla A en lugar de redirigir.
    # Pasamos los datos de sesión para que el header se vea bien.
    return render_template('A_control_logistica.html',
                           nombre=session.get('nombre_usuario'),
                           empresa=session.get('nombre_empresa'),
                           nit=session.get('empresa_id'))

# ==============================================================================
# ROUTER DEL BOTÓN AWS (LÓGICA DE DIRECCIONAMIENTO)
# ==============================================================================
@logistica_bp.route('/logistica/router/<modulo>')
def router_universal(modulo):
    usuario_id = session.get('usuario_id')
    if not usuario_id: return redirect('/')

    # 1. Obtener perfil real desde BD (Seguridad)
    perfil_real = ""
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT perfil FROM usuarios WHERE id = %s", (usuario_id,))
        row = cur.fetchone()
        cur.close()
        perfil_real = str(row[0]).strip().lower() if row else ""
    except:
        perfil_real = str(session.get('perfil', '')).strip().lower()

    # 2. Buscar reglas para el módulo solicitado (ej: "aws")
    reglas = ACCESO_MODULOS.get(modulo)
    if not reglas:
        flash("Módulo no configurado", "warning")
        return redirect(url_for('logistica_bp.home'))

    # 3. Buscar destino según el perfil
    archivo_destino = reglas.get(perfil_real)
    
    # Búsqueda flexible (por si el perfil en BD tiene mayúsculas o espacios)
    if not archivo_destino:
        for p_key, archivo in reglas.items():
            if p_key in perfil_real:
                archivo_destino = archivo
                break

    # 4. EJECUTAR REDIRECCIÓN EXACTA
    if archivo_destino:
        
        # CASO 1: OPERARIO (Ir a C)
        if archivo_destino == "C_bodegas.html":
            # Asegúrate que en C_bp_oper_bodegas.py la función se llame 'bodega_operativa'
            return redirect(url_for('oper_bodegas.bodega_operativa'))
            
        # CASO 2: JEFE (Ir a B)
        elif archivo_destino == "B_control_logistica.html":
             # CORRECCIÓN: Apuntamos a la función correcta de B_bp_bodegas.py
             return redirect(url_for('bodegas.control_logistica')) 

        # CASO 3: OTROS
        elif archivo_destino == "B_dashboard_tc.html":
            return redirect('/B_dashboard_tc.html')

        # Fallback (Renderizado directo si no es una ruta especial)
        return render_template(archivo_destino)
        
    else:
        # Si no tiene permiso, lo devolvemos al Menú A
        flash(f"ACCESO DENEGADO: Tu perfil '{perfil_real}' no tiene acceso.", "error")
        return redirect(url_for('logistica_bp.home'))