from flask import Blueprint, render_template, session, request, jsonify, flash, redirect, url_for, current_app
from datetime import datetime
from app import mysql, csrf
from app.forms import RegistroUsuarioForm
from app.utils import login_required_custom
from flask_bcrypt import Bcrypt
import traceback
import math

bcrypt = Bcrypt()

bp_901811727 = Blueprint('bp_901811727', __name__)

# ==============================================================================
# RUTAS DE GESTIÓN
# ==============================================================================

@bp_901811727.route('/901811727.html')
@login_required_custom
def panel_webmaster():
    form = RegistroUsuarioForm()
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT nit, nombre_comercial FROM empresas")
        empresas = cur.fetchall()
        cur.execute("SELECT nombre_comercial FROM empresas")
        clientes = cur.fetchall()
        cur.close()
        return render_template('901811727.html', nombre=session.get('nombre'), empresa=session.get('empresa'), form=form, empresas=empresas, clientes=clientes)
    except Exception as e:
        print("Error panel:", e)
        return "Error cargando panel", 500

@csrf.exempt
@bp_901811727.route('/registrar_empresa', methods=['POST'])
@login_required_custom
def registrar_empresa():
    nombre_comercial = request.form.get('nombre_comercial', '').strip()
    nit = request.form.get('nit', '').strip()
    if not nombre_comercial or not nit:
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT * FROM empresas WHERE nit = %s", (nit,))
        if cur.fetchone():
            return jsonify({'success': False, 'message': 'La empresa ya existe.'})
        cur.execute("INSERT INTO empresas (nit, nombre_comercial) VALUES (%s, %s)", (nit, nombre_comercial))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Empresa creada correctamente.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/registrar_perfil', methods=['POST'])
@login_required_custom
def registrar_perfil():
    empresa_nombre = request.form.get('empresa_select', '').strip()
    nit = request.form.get('nit', '').strip()
    operacion = request.form.get('operacion', '').strip()
    perfil = request.form.get('perfil', '').strip()
    if not all([empresa_nombre, nit, operacion, perfil]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT nit FROM empresas WHERE nombre_comercial = %s", (empresa_nombre,))
        empresa = cur.fetchone()
        if not empresa or str(empresa['nit']) != nit:
            return jsonify({'success': False, 'message': 'Empresa no válida o NIT incorrecto.'})
        cur.execute("SELECT * FROM perfiles WHERE nit = %s AND operacion = %s AND perfil = %s", (nit, operacion, perfil))
        if cur.fetchone():
            return jsonify({'success': False, 'message': 'El perfil ya existe.'})
        cur.execute("INSERT INTO perfiles (nit, operacion, perfil) VALUES (%s, %s, %s)", (nit, operacion, perfil))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Perfil creado correctamente.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/obtener_perfiles')
@login_required_custom
def obtener_perfiles():
    empresa_id = request.args.get('empresa_id', '').strip()
    operacion = request.args.get('operacion', '').strip()
    if not empresa_id or not operacion:
        return jsonify({'perfiles': []})
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT DISTINCT perfil FROM perfiles WHERE nit = %s AND operacion = %s", (empresa_id, operacion))
        perfiles = [row['perfil'] for row in cur.fetchall()]
        cur.close()
        return jsonify({'perfiles': perfiles})
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'perfiles': []})

@csrf.exempt
@bp_901811727.route('/registrar_usuario', methods=['POST'])
@login_required_custom
def registrar_usuario():
    data = request.form
    cedula = data.get('cedula', '').strip()
    nombre = data.get('nombre', '').strip()
    password = data.get('password', '').strip()
    if not all([cedula, nombre, password]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
        if cur.fetchone():
            return jsonify({'success': False, 'message': 'El usuario ya existe.'})
        password_hashed = bcrypt.generate_password_hash(password).decode('utf-8')
        cur.execute("""
            INSERT INTO usuarios (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (cedula, nombre, password_hashed, data.get('tipo_usuario'), data.get('clase'), 
              data.get('perfil'), data.get('empresa_id'), data.get('empresa_select')))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Usuario creado.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/consultar_proveedores', methods=['POST'])
@login_required_custom
def consultar_proveedores():
    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id:
        return jsonify({"success": False, "message": "Empresa no especificada"})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT proveedor, id_proveedor, email1, email2 FROM proveedores WHERE id_empresa = %s", (empresa_id,))
        data = [{"proveedor": r[0], "id_proveedor": r[1], "email1": r[2], "email2": r[3]} for r in cur.fetchall()]
        return jsonify({"success": True, "proveedores": data})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/obtener_periodo', methods=['POST'])
@login_required_custom
def obtener_periodo():
    p = request.form.get('periodo')
    return jsonify({'success': True, 'periodo': p, 'fecha_inicio': request.form.get('fecha_inicio'), 'fecha_fin': request.form.get('fecha_fin')})


# ==============================================================================
# LÓGICA DE INFORMES Y ESTADÍSTICAS (AJUSTADA A FÓRMULA DEL USUARIO)
# ==============================================================================

def _procesar_resultados_glp(resultados, tipo_informe, periodo):
    if not resultados:
        return None

    def safe_float(val):
        if val is None: return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    # Inicializar contadores (KG)
    math_saldo_inicial_kg_global = 0.0
    math_saldo_final_kg_global = 0.0
    math_ingresos_kg = 0.0
    math_consumo_parcial_kg = 0.0 # Suma de neto_gastado (clase='egreso')
    
    total_pollitos_global = 0
    lotes_procesados_global = set()

    series = { 'fechas': [], 'kg_pollito': [], 'saldo_inicial': [], 'saldo_final': [], 'ingresos': [] }
    
    # Estructura para Tabla Resumen
    granjas_data = {}

    resultados.sort(key=lambda x: str(x.get('fecha')))

    for row in resultados:
        fecha_str = str(row.get('fecha'))
        clase = str(row.get('clase') or '').lower().strip()
        lote_id = row.get('lote')
        ubicacion = row.get('ubicacion') or 'Desconocida'

        # Extracción de valores (TODO EN KG)
        val_kg_saldo = safe_float(row.get('saldo_estimado_kg'))
        masa_fact = safe_float(row.get('masa_kg_facturada'))
        neto_gast = safe_float(row.get('neto_gastado'))
        kg_pollo  = safe_float(row.get('kg_pollito'))
        pollitos  = int(row.get('pollitos') or 0)

        # 1. Series Gráficas
        series['fechas'].append(fecha_str)
        series['kg_pollito'].append(kg_pollo)
        series['saldo_inicial'].append(val_kg_saldo if clase == 'saldo inicial' else None)
        series['saldo_final'].append(val_kg_saldo if clase == 'saldo final' else None)
        series['ingresos'].append(masa_fact if clase == 'ingreso' else None)

        # 2. Acumuladores Globales
        if clase == 'saldo inicial':
            math_saldo_inicial_kg_global += val_kg_saldo
        elif clase == 'saldo final':
            math_saldo_final_kg_global += val_kg_saldo
        elif clase == 'ingreso':
            math_ingresos_kg += masa_fact
        elif clase == 'egreso':
            math_consumo_parcial_kg += neto_gast

        if lote_id and lote_id not in lotes_procesados_global:
            total_pollitos_global += pollitos
            lotes_procesados_global.add(lote_id)

        # 3. Datos por Granja (Tabla Resumen)
        if ubicacion not in granjas_data:
            granjas_data[ubicacion] = {
                'inicial': 0.0, 'final': 0.0, 'ingresos': 0.0, 'parciales': 0.0,
                'pollitos': 0, 'lotes': set()
            }
        
        d = granjas_data[ubicacion]
        if clase == 'saldo inicial': d['inicial'] += val_kg_saldo
        elif clase == 'saldo final': d['final'] += val_kg_saldo
        elif clase == 'ingreso': d['ingresos'] += masa_fact
        elif clase == 'egreso': d['parciales'] += neto_gast
        
        if lote_id and lote_id not in d['lotes']:
            d['pollitos'] += pollitos
            d['lotes'].add(lote_id)

    # --- CÁLCULOS SEGÚN TIPO DE PERIODO ---
    
    kpis = {}
    
    if periodo == 'Actual':
        # Estatus ACTIVO
        kpis = {
            "card1_label": "Saldo Inicial (kg)",
            "card1_value": math_saldo_inicial_kg_global,
            
            "card2_label": "Pedidos Gas (kg)",
            "card2_value": math_ingresos_kg,
            
            "card3_label": "Consumos Parciales (kg)",
            "card3_value": math_consumo_parcial_kg, # Suma de neto_gastado
            
            "card4_label": "---",
            "card4_value": 0,
            
            "card5_label": "Pollitos Activos",
            "card5_value": total_pollitos_global
        }
        
    else:
        # Periodo Personalizado (Estatus INACTIVO)
        
        # FÓRMULA ESPECÍFICA SOLICITADA POR EL USUARIO:
        # Consumo = Suma(Inicial) + Suma(Ingresos) - Suma(Egresos_Parciales) - Suma(Final)
        consumo_total = (math_saldo_inicial_kg_global + math_ingresos_kg) - math_consumo_parcial_kg - math_saldo_final_kg_global
        
        rendimiento = consumo_total / total_pollitos_global if total_pollitos_global > 0 else 0.0
        
        kpis = {
            "card1_label": "Consumo Total (kg)",
            "card1_value": consumo_total,
            
            "card2_label": "Rendimiento (kg/ave)",
            "card2_value": rendimiento,
            
            "card3_label": "Saldo Final Total (kg)",
            "card3_value": math_saldo_final_kg_global,
            
            "card4_label": "---",
            "card4_value": 0,
            
            "card5_label": "Total Pollitos",
            "card5_value": total_pollitos_global
        }

    # --- TABLA RESUMEN ---
    tabla_resumen = []
    lista_rendimientos = []

    for nombre_granja, datos in granjas_data.items():
        consumo_granja = 0.0
        if periodo == 'Actual':
            # En periodo actual, el consumo observable son las lecturas parciales
            consumo_granja = datos['parciales']
        else:
            # Fórmula específica por granja: Ini + Ing - Parciales - Fin
            consumo_granja = (datos['inicial'] + datos['ingresos']) - datos['parciales'] - datos['final']
        
        rend_granja = 0.0
        if datos['pollitos'] > 0:
            rend_granja = consumo_granja / datos['pollitos']
            lista_rendimientos.append(rend_granja)

        tabla_resumen.append({
            'granja': nombre_granja,
            'total_kg': consumo_granja,
            'pollitos': datos['pollitos'],
            'kg_pollito': rend_granja
        })

    media = 0.0
    desviacion = 0.0
    n = len(lista_rendimientos)
    if n > 0:
        media = sum(lista_rendimientos) / n
        if n > 1:
            varianza = sum((x - media) ** 2 for x in lista_rendimientos) / (n - 1)
            desviacion = math.sqrt(varianza)

    return {
        "kpis": kpis,
        "series": series,
        "tabla_resumen": tabla_resumen,
        "estadisticas": { "media": media, "desviacion": desviacion },
        "periodo_tipo": periodo
    }

@csrf.exempt
@bp_901811727.route('/generar_informe', methods=['POST'])
@login_required_custom
def generar_informe():
    cursor = None
    try:
        data = request.get_json() or {}
        tipo_informe = data.get('tipo_informe')
        periodo = data.get('periodo')
        fecha_ini = data.get('fecha_inicio')
        fecha_fin = data.get('fecha_fin')
        ubicacion = data.get('ubicacion') 
        empresa_id = data.get('empresa_id')

        if not empresa_id or not tipo_informe:
            return jsonify({"success": False, "message": "Faltan datos obligatorios."}), 400

        cursor = mysql.connection.cursor()
        wheres = ["WHERE c.id_empresa = %s"]
        params = [empresa_id]

        if tipo_informe == 'zona' and ubicacion:
            cursor.execute("SELECT DISTINCT ubicacion FROM tanques_sedes WHERE zona = %s AND empresa_id = %s", (ubicacion, empresa_id))
            granjas = [r['ubicacion'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
            if not granjas:
                return jsonify({"success": False, "message": f"Zona '{ubicacion}' sin granjas."})
            placeholders = ', '.join(['%s'] * len(granjas))
            wheres.append(f"AND c.ubicacion IN ({placeholders})")
            params.extend(granjas)
        elif tipo_informe == 'granja' and ubicacion:
            wheres.append("AND c.ubicacion = %s")
            params.append(ubicacion)

        if periodo == 'Personalizado' and fecha_ini and fecha_fin:
            wheres.append("AND c.fecha BETWEEN %s AND %s")
            params.append(fecha_ini)
            params.append(fecha_fin)
            # Para Personalizado, SIEMPRE INACTIVO
            wheres.append("AND c.estatus_lote = 'INACTIVO'")
                
        elif periodo == 'Actual':
            # Para Actual, SIEMPRE ACTIVO
            wheres.append("AND c.estatus_lote = 'ACTIVO'")

        sql = """
            SELECT c.fecha, c.ubicacion, c.lote, c.estatus_lote, c.operacion, c.clase, 
                   c.saldo_estimado_kg, c.saldo_estimado_galones,
                   c.pollitos, c.kg_pollito, c.masa_kg_facturada, c.neto_gastado 
            FROM cardex_glp c
        """
        final_sql = f"{sql} {' '.join(wheres)} ORDER BY c.fecha ASC"
        
        cursor.execute(final_sql, tuple(params))
        rows = cursor.fetchall()

        raw_results = []
        if rows:
            columns = [col[0] for col in cursor.description]
            for row in rows:
                if isinstance(row, dict):
                    raw_results.append(row)
                else:
                    raw_results.append(dict(zip(columns, row)))

        cursor.close()

        datos = _procesar_resultados_glp(raw_results, tipo_informe, periodo)
        if not datos:
            return jsonify({"success": False, "message": "No hay datos para mostrar."})

        return jsonify({"success": True, "data": datos})

    except Exception as e:
        if cursor: cursor.close()
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_901811727.route('/obtener_ubicaciones', methods=['POST'])
@login_required_custom
def obtener_ubicaciones():
    cursor = None
    try:
        data = request.get_json()
        tipo = data.get('tipo')
        empresa_id = data.get('empresa_id')

        if not empresa_id or not tipo:
            return jsonify({"success": False})

        cursor = mysql.connection.cursor()
        query = ""
        col = ""
        if tipo == "granja":
            query = "SELECT DISTINCT ubicacion FROM cardex_glp WHERE id_empresa = %s ORDER BY ubicacion ASC"
            col = "ubicacion"
        elif tipo == "zona":
            query = "SELECT DISTINCT zona FROM tanques_sedes WHERE empresa_id = %s ORDER BY zona ASC"
            col = "zona"
        else:
            return jsonify({"success": True, "ubicaciones": []})

        cursor.execute(query, (empresa_id,))
        rows = cursor.fetchall()
        
        results = []
        for r in rows:
            val = r.get(col) if isinstance(r, dict) else r[0]
            if val: results.append(val)

        cursor.close()
        return jsonify({"success": True, "ubicaciones": sorted(list(set(results)))})
    except Exception as e:
        if cursor: cursor.close()
        print("Error ubicaciones:", e)
        return jsonify({"success": False, "message": str(e)}), 500