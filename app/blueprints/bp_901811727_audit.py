# bp_901811727_audit.py
from flask import Blueprint, request, jsonify, current_app
from app import mysql, csrf
from app.utils import login_required_custom
import time
import psutil # Requiere: pip install psutil

bp_audit = Blueprint('bp_901811727_audit', __name__)

# ==============================================================================
# AUDITORÍA DE EVENTOS (BITÁCORA EN VIVO)
# ==============================================================================

@csrf.exempt
@bp_audit.route('/obtener_audit_log', methods=['POST'])
@login_required_custom
def obtener_audit_log():
    empresa_id = request.form.get('empresa_id') 
    if not empresa_id:
        return jsonify({'success': False, 'logs': []})

    try:
        cur = mysql.connection.cursor()
        
        # 1. Buscamos el nombre de la empresa para rescatar registros huérfanos (con ID 0)
        cur.execute("SELECT nombre_comercial FROM empresas WHERE nit = %s", (empresa_id,))
        row_emp = cur.fetchone()
        emp_nombre = ""
        if row_emp:
            emp_nombre = row_emp['nombre_comercial'] if isinstance(row_emp, dict) else row_emp[0]

        # 2. Búsqueda blindada: Busca por ID o por Nombre (para atrapar los que tienen ID 0)
        cur.execute("""
            SELECT fecha, modulo, usuario, accion, detalle, nivel 
            FROM audit_log 
            WHERE empresa_id = %s OR empresa_nombre = %s
            ORDER BY fecha DESC 
            LIMIT 100
        """, (empresa_id, emp_nombre))
        
        logs = cur.fetchall()
        cur.close()
        
        data = []
        nombres_cols = ['fecha', 'modulo', 'usuario', 'accion', 'detalle', 'nivel']

        for row in logs:
            if isinstance(row, tuple):
                r = dict(zip(nombres_cols, row))
            else:
                r = row 
            
            fecha_val = r.get('fecha')
            if fecha_val:
                fecha_str = fecha_val.strftime('%Y-%m-%d %H:%M:%S')
            else:
                fecha_str = 'N/A'
            
            data.append({
                'fecha': fecha_str,
                'modulo': r.get('modulo'),
                'usuario': r.get('usuario'),
                'accion': r.get('accion'),
                'detalle': r.get('detalle'),
                'nivel': r.get('nivel')
            }) 
            
        return jsonify({'success': True, 'logs': data})

    except Exception as e:
        print(f"Error Audit Log: {e}")
        return jsonify({'success': False, 'message': str(e)})

# ==============================================================================
# DIAGNÓSTICO DEL SISTEMA (HEALTH CHECK)
# ==============================================================================

@bp_audit.route('/api/health_check', methods=['GET'])
@login_required_custom
def health_check():
    health_data = {
        "status": "healthy",
        "database": {"status": "error", "latency_ms": 0},
        "server": {"cpu_percent": psutil.cpu_percent(), "ram_percent": psutil.virtual_memory().percent},
        "blueprints_activos": len(current_app.blueprints)
    }
    
    # Ping a BD para medir latencia
    try:
        start_time = time.time()
        cur = mysql.connection.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        health_data["database"]["latency_ms"] = round((time.time() - start_time) * 1000, 2)
        health_data["database"]["status"] = "ok"
    except Exception:
        health_data["status"] = "degraded"
        
    return jsonify(health_data)