# MÓDULO: GERENCIA | SUBMÓDULO: HUB_BAQUIA | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom

B_bp_hub_gerencia_baquia = Blueprint('B_bp_hub_gerencia_baquia', __name__)

def _obtener_matriz_unidades_negocio():
    """
    Arquitectura Data-Driven: Centraliza la estructura de Unidades de Negocio (UN)
    para Baquía-ems SAS. Resuelve endpoints canónicos registrados en Flask.
    """
    return [
        {
            "unidad": "UN BQA-ONE (DIVISIÓN SOFTWARE)",
            "color": "primary",
            "icono": "💻",
            "submodulos": [
                {
                    "nombre": "GESTIÓN CLIENTES BQA-ONE",
                    "endpoint": "bp_901811727_main.panel_admin",
                    "icono": "🏢",
                    "descripcion": "Administración central de empresas clientes, asignación de módulos y gestión de usuarios.",
                    "estado": "activo"
                },
                {
                    "nombre": "MONITOREO BQA-ONE",
                    "endpoint": "bp_901811727_main.panel_audit",
                    "icono": "👁️",
                    "descripcion": "Bitácora en tiempo real (Audit.log), diagnóstico de latencia y health check del servidor.",
                    "estado": "activo"
                }
            ]
        },
        {
            "unidad": "UN ENERGÉTICO",
            "color": "success",
            "icono": "⚡",
            "submodulos": [
                {
                    "nombre": "GESTIÓN ENERGÉTICA",
                    "endpoint": "bp_901811727_main.panel_energia",
                    "icono": "🔥",
                    "descripcion": "Control operativo de GLP, validación de tanqueos con fotos testigo, kárdex y benchmarking.",
                    "estado": "activo"
                }
            ]
        },
        {
            "unidad": "UN OPERACIONES",
            "color": "warning",
            "icono": "⚙️",
            "submodulos": [
                {
                    "nombre": "GESTIÓN DE MANTENIMIENTO CLIENTES",
                    "endpoint": "controlador_mantenimiento.vista_controlador",
                    "icono": "🔧",
                    "descripcion": "Programación de visitas en campo, actas de inspección técnica y certificación de clientes.",
                    "estado": "activo"
                }
            ]
        },
        {
            "unidad": "ADMINISTRACIÓN BAQUÍA-EMS SAS (CONTROL Y GESTIÓN)",
            "color": "info",
            "icono": "🏛️",
            "submodulos": [
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
            "unidad": "ADMINISTRACIÓN BAQUÍA-EMS SAS (OPERADORES EN PISO)",
            "color": "dark",
            "icono": "🛠️",
            "submodulos": [
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
        },
        {
            "unidad": "FUTURAS UNIDADES DE NEGOCIO (PROYECCIÓN)",
            "color": "secondary",
            "icono": "🚀",
            "submodulos": [
                {
                    "nombre": "CONSTRUCCIÓN DE OBRAS",
                    "endpoint": None,
                    "icono": "🏗️",
                    "descripcion": "Gestión de proyectos de infraestructura civil, montajes electromecánicos y obras industriales.",
                    "estado": "proximamente"
                },
                {
                    "nombre": "MANTENIMIENTO",
                    "endpoint": None,
                    "icono": "🔧",
                    "descripcion": "Servicios técnicos integrales de mantenimiento preventivo y correctivo para clientes.",
                    "estado": "proximamente"
                }
            ]
        }
    ]

@B_bp_hub_gerencia_baquia.route('/hub_gerencia_baquia.html')
@login_required_custom
def hub_gerencia():
    perfil = str(session.get('perfil', '')).strip().lower()
    empresa_id = str(session.get('empresa_id', '')).strip()

    perfiles_autorizados = ['gerente_bqa', 'webmaster', 'webmaster_admin', 'admin_general']
    if perfil not in perfiles_autorizados and empresa_id != '901811727':
        flash("Acceso denegado: No posees privilegios gerenciales corporativos.", "danger")
        return redirect(url_for('index'))

    unidades_negocio = _obtener_matriz_unidades_negocio()

    return render_template(
        'B_hub_gerencia_baquia.html',
        nombre=session.get('nombre', 'Gerente General'),
        usuario=session.get('nombre', 'Usuario'),
        empresa=session.get('empresa', 'Baquía-ems SAS'),
        nit=empresa_id,
        unidades_negocio=unidades_negocio
    )