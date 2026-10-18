# MÓDULO: ADMINISTRACION | SUBMÓDULO: HUB_BAQUIA | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom

B_bp_hub_administracion_baquia = Blueprint('B_bp_hub_administracion_baquia', __name__)

def _obtener_modulos_administracion():
    """
    Arquitectura Data-Driven: Matriz de submódulos asignados a la administración 
    interna de Baquía-ems SAS con resolución directa de endpoints canónicos.
    """
    return [
        {
            "categoria": "HERRAMIENTAS DE CONTROL Y GESTIÓN",
            "color": "primary",
            "modulos": [
                {
                    "nombre": "Nómina y RRHH",
                    "endpoint": "controlador_recurso_humano.dashboard",
                    "icono": "👥",
                    "descripcion": "Gestión de personal, control de asistencia y nómina interna.",
                    "estado": "activo"
                },
                {
                    "nombre": "Contabilidad y Finanzas",
                    "endpoint": "controlador_contabilidad.dashboard",
                    "icono": "📊",
                    "descripcion": "Balances, registros contables y estados financieros.",
                    "estado": "activo"
                },
                {
                    "nombre": "Facturación y Cartera",
                    "endpoint": "controlador_facturacion.dashboard",
                    "icono": "🧾",
                    "descripcion": "Emisión de facturas electrónicas y control de cuentas por cobrar.",
                    "estado": "activo"
                },
                {
                    "nombre": "Gestión de Ventas",
                    "endpoint": "controlador_ventas.dashboard",
                    "icono": "📈",
                    "descripcion": "Gestión comercial corporativa, cotizaciones y CRM de clientes.",
                    "estado": "activo"
                }
            ]
        },
        {
            "categoria": "MÓDULOS OPERATIVOS EN PISO",
            "color": "dark",
            "modulos": [
                {
                    "nombre": "Operador Contable",
                    "endpoint": "operador_contable.dashboard_operador",
                    "icono": "📉",
                    "descripcion": "Ingreso de registros diarios y causaciones contables.",
                    "estado": "activo"
                },
                {
                    "nombre": "Operador de Facturación",
                    "endpoint": "operador_facturacion.dashboard_operador",
                    "icono": "🖨️",
                    "descripcion": "Generación operativa de pre-facturas y notas.",
                    "estado": "activo"
                },
                {
                    "nombre": "Operador de Ventas",
                    "endpoint": "operador_ventas.dashboard_operador",
                    "icono": "🛒",
                    "descripcion": "Ejecución comercial y registro de cotizaciones en piso.",
                    "estado": "activo"
                }
            ]
        }
    ]

@B_bp_hub_administracion_baquia.route('/hub_administracion_baquia.html')
@login_required_custom
def hub_administracion():
    perfil = str(session.get('perfil', '')).strip().lower()
    empresa_id = str(session.get('empresa_id', '')).strip()

    if empresa_id != '901811727':
        flash("Acceso denegado: Este hub es de uso exclusivo interno de Baquía-ems SAS.", "danger")
        return redirect(url_for('index'))

    perfiles_autorizados = [
        'gerente_bqa', 
        'webmaster', 
        'webmaster_admin', 
        'admin_general',
        'webmaster_administrador_baquia',
        'controlador_recurso_humano', 
        'controlador_contabilidad',
        'controlador_facturacion', 
        'controlador_ventas',
        'operador_contable', 
        'operador_facturacion', 
        'operador_ventas'
    ]
    
    if perfil not in perfiles_autorizados:
        flash("Acceso denegado: Tu perfil no tiene autorización para acceder a este Hub.", "danger")
        return redirect(url_for('index'))

    modulos_admin = _obtener_modulos_administracion()

    return render_template(
        'B_hub_administracion_baquia.html',
        nombre=session.get('nombre', 'Usuario'),
        empresa=session.get('empresa', 'Baquía-ems SAS'),
        nit=empresa_id,
        modulos_admin=modulos_admin
    )